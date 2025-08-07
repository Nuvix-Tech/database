import { Client, PoolConfig } from "pg";
import { BaseAdapter } from "./base.js";
import { PostgresClient } from "./postgres.js";
import { AttributeEnum, EventsEnum, IndexEnum, PermissionEnum, RelationEnum, RelationSideEnum } from "@core/enums.js";
import { CreateCollectionOptions, IAdapter } from "./interface.js";
import { DatabaseException } from "@errors/base.js";
import { Database, PopulateQuery, ProcessedQuery } from "@core/database.js";
import { Doc } from "@core/doc.js";
import { NotFoundException } from "@errors/index.js";
import { Attribute, Collection, RelationOptions } from "@validators/schema.js";
import { ColumnInfo, CreateAttribute, CreateIndex, UpdateAttribute } from "./types.js";
import { Query, QueryType } from "@core/query.js";
import { Authorization } from "@utils/authorization.js";

export class Adapter extends BaseAdapter implements IAdapter {
    protected client: PostgresClient;

    constructor(client: PoolConfig | Client) {
        super();
        this.client = new PostgresClient(client);
    }

    async create(name: string): Promise<void> {
        name = this.quote(name);
        if (await this.exists(name)) return;

        let sql = `CREATE SCHEMA ${name};`;
        sql = this.trigger(EventsEnum.DatabaseCreate, sql);

        await this.client.query(sql);
    }

    async delete(name: string): Promise<void> {
        name = this.quote(name);
        await this.client.query(`DROP SCHEMA IF EXISTS ${name};`);
    }

    async createCollection({ name, attributes, indexes }: CreateCollectionOptions): Promise<void> {
        name = this.sanitize(name);
        const mainTable = this.getSQLTable(name);
        const attributeSql: string[] = [];
        const indexSql: string[] = [];
        const attributeHash: Record<string, Attribute> = {};

        attributes.forEach(attribute => {
            const id = this.sanitize(attribute.getId());
            if (attribute.get('type') === AttributeEnum.Virtual) {
                return;
            }

            if (attribute.get('type') === AttributeEnum.Relationship) {
                const options = attribute.get('options', {}) as Record<string, any>;
                const relationType = options['relationType'] ?? null;
                const twoWay = options['twoWay'] ?? false;
                const side = options['side'] ?? null;

                if (
                    relationType === RelationEnum.ManyToMany
                    || (relationType === RelationEnum.OneToOne && !twoWay && side === 'child')
                    || (relationType === RelationEnum.OneToMany && side === RelationSideEnum.Parent)
                    || (relationType === RelationEnum.ManyToOne && side === RelationSideEnum.Child)
                ) {
                    return;
                }
            }

            attributeHash[id] = attribute.toObject();
            const type = this.getSQLType(
                attribute.get('type'),
                attribute.get('size'),
                attribute.get('array')
            );

            let sql = `${this.quote(id)} ${type}`;
            attributeSql.push(sql);
        });

        indexes?.forEach(index => {
            const indexId = index.getId();
            const indexType = index.get('type');
            const indexAttributes = index.get('attributes') as string[];
            const orders = index.get('orders') || [];

            const isFulltext = indexType === IndexEnum.FullText;
            const hasArrayAttribute = indexAttributes.some(attrKey => {
                const metadata = attributeHash[attrKey];
                return metadata?.array;
            });

            let usingClause = '';
            if (isFulltext || hasArrayAttribute) {
                usingClause = 'USING GIN';
            }

            const formattedIndexAttributes = indexAttributes.map((attributeKey, i) => {
                const pgKey = `"${this.sanitize(this.getInternalKeyForAttribute(attributeKey))}"`;
                const order = (orders[i] && !isFulltext) ? ` ${orders[i]}` : '';

                if (isFulltext) {
                    return `to_tsvector('english', ${pgKey})`;
                }

                return `${pgKey}${order}`;
            });

            // For multi-column full-text indexes, we must join the `to_tsvector` calls
            let attributesForSql = formattedIndexAttributes.join(', ');
            if (isFulltext && formattedIndexAttributes.length > 1) {
                attributesForSql = formattedIndexAttributes.join(' || ');
            }

            if (this.$sharedTables && !isFulltext) {
                const pgTenantKey = `"${this.sanitize('_tenant')}"`;
                attributesForSql = `${pgTenantKey}, ${attributesForSql}`;
            }

            const uniqueClause = isFulltext ? '' : (indexType === IndexEnum.Unique ? 'UNIQUE ' : '');

            const pgIndexId = `"${this.getIndexName(name, this.sanitize(indexId))}"`;
            const sql = `CREATE ${uniqueClause}INDEX ${pgIndexId} ON ${mainTable} ${usingClause} (${attributesForSql});`;

            indexSql.push(sql);
        });

        const mainTableColumns = [
            `"_id" BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY`,
            `"_uid" VARCHAR(255) NOT NULL`,
            `"_createdAt" TIMESTAMP WITH TIME ZONE DEFAULT NULL`,
            `"_updatedAt" TIMESTAMP WITH TIME ZONE DEFAULT NULL`,
            `"_permissions" TEXT[] DEFAULT '{}'`,
            ...attributeSql
        ];

        let primaryKeyDefinition: string;
        const tenantCol = this.quote('_tenant');

        if (this.$sharedTables) {
            mainTableColumns.splice(1, 0, `${tenantCol} BIGINT DEFAULT NULL`);
            primaryKeyDefinition = `PRIMARY KEY ("_id", ${tenantCol})`;
        } else {
            primaryKeyDefinition = `PRIMARY KEY ("_id")`;
        }

        const columnsAndConstraints = mainTableColumns.join(',\n');
        let tableSql = `
            CREATE TABLE ${mainTable} (
                ${columnsAndConstraints},
                ${primaryKeyDefinition}
            );
        `;

        const postTableIndexes: string[] = [];
        if (this.$sharedTables) {
            postTableIndexes.push(`CREATE UNIQUE INDEX "${name}_uid_tenant" ON ${mainTable} ("_uid", ${tenantCol});`);
            postTableIndexes.push(`CREATE INDEX "${name}_created_at_tenant" ON ${mainTable} (${tenantCol}, "_createdAt");`);
            postTableIndexes.push(`CREATE INDEX "${name}_updated_at_tenant" ON ${mainTable} (${tenantCol}, "_updatedAt");`);
            postTableIndexes.push(`CREATE INDEX "${name}_tenant_id" ON ${mainTable} (${tenantCol}, "_id");`);
        } else {
            postTableIndexes.push(`CREATE UNIQUE INDEX "${name}_uid" ON ${mainTable} ("_uid");`);
            postTableIndexes.push(`CREATE INDEX "${name}_created_at" ON ${mainTable} ("_createdAt");`);
            postTableIndexes.push(`CREATE INDEX "${name}_updated_at" ON ${mainTable} ("_updatedAt");`);
        }
        postTableIndexes.push(`CREATE INDEX "${name}_permissions_gin_idx" ON ${mainTable} USING GIN ("_permissions");`);

        tableSql = this.trigger(EventsEnum.CollectionCreate, tableSql);

        const permissionsTableName = this.getSQLTable(name + '_perms');

        const permissionsTableColumns = [
            `"_id" BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY`,
            `"_type" VARCHAR(12) NOT NULL`,
            `"_permissions" TEXT[] NOT NULL DEFAULT '{}'`,
            `"_document" BIGINT NOT NULL`,
            `FOREIGN KEY ("_document") REFERENCES ${mainTable}("_id") ON DELETE CASCADE`
        ];
        const postPermissionsTableIndexes: string[] = [];
        let permissionsPrimaryKeyDefinition: string;

        if (this.$sharedTables) {
            permissionsTableColumns.splice(1, 0, `${tenantCol} BIGINT DEFAULT NULL`);
            permissionsPrimaryKeyDefinition = `PRIMARY KEY ("_id", ${tenantCol})`;

            postPermissionsTableIndexes.push(`CREATE UNIQUE INDEX "${name}_perms_index1" ON ${permissionsTableName} ("_document", ${tenantCol}, "_type");`);
            postPermissionsTableIndexes.push(`CREATE INDEX "${name}_perms_tenant" ON ${permissionsTableName} (${tenantCol});`);
        } else {
            permissionsPrimaryKeyDefinition = `PRIMARY KEY ("_id")`;
            postPermissionsTableIndexes.push(`CREATE UNIQUE INDEX "${name}_perms_index1" ON ${permissionsTableName} ("_document", "_type");`);
        }
        postPermissionsTableIndexes.push(`CREATE INDEX "${name}_perms_permissions_gin_idx" ON ${permissionsTableName} USING GIN ("_permissions");`);

        const permissionsColumnsAndConstraints = permissionsTableColumns.join(',\n');
        let permissionsTable = `
            CREATE TABLE ${permissionsTableName} (
                ${permissionsColumnsAndConstraints},
                ${permissionsPrimaryKeyDefinition}
            );
        `;

        permissionsTable = this.trigger(EventsEnum.PermissionsCreate, permissionsTable);
        await this.$client.transaction(async () => {
            await this.client.query(tableSql);
            for (const sql of postTableIndexes) {
                await this.client.query(sql);
            }

            for (const sql of indexSql) {
                await this.client.query(sql);
            }

            await this.client.query(permissionsTable);
            for (const sql of postPermissionsTableIndexes) {
                await this.client.query(sql);
            }
        })
    }

    public async getSizeOfCollectionOnDisk(collection: string): Promise<number> {
        collection = this.sanitize(collection);
        const collectionTableName = this.quote(`${this.$namespace}_${collection}`);
        const permissionsTableName = this.quote(`${this.$namespace}_${collection}_perms`);

        const sql = `
            SELECT
                pg_total_relation_size(${collectionTableName}::regclass) AS collection_size,
                pg_total_relation_size(${permissionsTableName}::regclass) AS permissions_size;
        `;

        try {
            const [rows]: any = await this.client.query(sql);
            const collectionSize = Number(rows[0]?.collection_size ?? 0);
            const permissionsSize = Number(rows[0]?.permissions_size ?? 0);
            return collectionSize + permissionsSize;
        } catch (e: any) {
            if (e.message.includes('relation') && e.message.includes('does not exist')) {
                return 0;
            }
            this.processException(e, `Failed to get size of collection ${collection} on disk: ${e.message}`);
        }
    }

    public async getSizeOfCollection(collection: string): Promise<number> {
        collection = this.sanitize(collection);
        const collectionTableName = this.quote(`${this.$namespace}_${collection}`);
        const permissionsTableName = this.quote(`${this.$namespace}_${collection}_perms`);

        const sql = `
            SELECT
            pg_table_size(${collectionTableName}::regclass) + pg_indexes_size(${collectionTableName}::regclass) AS collection_size,
            pg_table_size(${permissionsTableName}::regclass) + pg_indexes_size(${permissionsTableName}::regclass) AS permissions_size;
        `;

        try {
            const [rows]: any = await this.client.query(sql);
            const collectionSize = Number(rows[0]?.collection_size ?? 0);
            const permissionsSize = Number(rows[0]?.permissions_size ?? 0);
            return collectionSize + permissionsSize;
        } catch (e: any) {
            if (e.message.includes('relation') && e.message.includes('does not exist')) {
                return 0;
            }
            this.processException(e, `Failed to get size of collection ${collection}: ${e.message}`);
        }
    }

    public async deleteCollection(id: string): Promise<void> {
        const permissionsTableName = this.getSQLTable(this.sanitize(id + '_perms'));
        const collectionTableName = this.getSQLTable(this.sanitize(id));

        let dropPermsSql = `DROP TABLE IF EXISTS ${permissionsTableName} CASCADE;`;
        dropPermsSql = this.trigger(EventsEnum.CollectionDelete, dropPermsSql);

        let dropCollectionSql = `DROP TABLE IF EXISTS ${collectionTableName} CASCADE;`;
        dropCollectionSql = this.trigger(EventsEnum.CollectionDelete, dropCollectionSql);

        try {
            await this.client.query(dropPermsSql);
            await this.client.query(dropCollectionSql);
        } catch (e: any) {
            this.processException(e, `Failed to delete collection ${id}`);
        }
    }

    public async analyzeCollection(collection: string): Promise<boolean> {
        const name = this.sanitize(collection);
        const tableName = this.getSQLTable(name);

        const sql = `ANALYZE ${tableName}`;

        try {
            await this.client.query(sql);
            return true;
        } catch (e: any) {
            this.processException(e, `Failed to analyze collection ${collection}`);
        }
    }

    public async createAttribute(
        { key: name, collection, size, array, type }: CreateAttribute
    ): Promise<void> {
        if (!name || !collection || !type) {
            throw new DatabaseException("Failed to create attribute: name, collection, and type are required");
        }

        const sqlType = this.getSQLType(type, size, array);
        const table = this.getSQLTable(collection);

        let sql = `
                ALTER TABLE ${table}
                ADD COLUMN ${this.$.quote(name)} ${sqlType}
            `;
        sql = this.trigger(EventsEnum.AttributeCreate, sql);

        try {
            await this.client.query(sql);
        } catch (e: any) {
            this.processException(e, `Failed to create attribute '${name}' in collection '${collection}'`);
        }
    }

    public async createAttributes(
        collection: string,
        attributes: Omit<CreateAttribute, 'collection'>[]
    ): Promise<void> {
        if (!Array.isArray(attributes) || attributes.length === 0) {
            throw new DatabaseException("Failed to create attributes: attributes must be a non-empty array");
        }

        const parts: string[] = [];

        for (const attr of attributes) {
            if (!attr.key || !attr.type) {
                throw new DatabaseException("Failed to create attribute: name and type are required");
            }

            const sqlType = this.getSQLType(attr.type, attr.size, attr.array);
            parts.push(`${this.quote(attr.key)} ${sqlType}`);
        }

        const columns = parts.join(', ADD COLUMN ');
        const table = this.getSQLTable(collection);
        let sql = `
                ALTER TABLE ${table}
                ADD COLUMN ${columns}
            `;

        sql = this.trigger(EventsEnum.AttributesCreate, sql);

        try {
            await this.client.query(sql);
        } catch (e: any) {
            this.processException(e, `Failed to create attributes in collection '${collection}'`);
        }
    }

    public async renameAttribute(
        collection: string,
        oldName: string,
        newName: string
    ): Promise<void> {
        if (!oldName || !newName || !collection) {
            throw new DatabaseException("Failed to rename attribute: oldName, newName, and collection are required");
        }

        const table = this.getSQLTable(collection);
        let sql = `
                ALTER TABLE ${table}
                RENAME COLUMN ${this.quote(oldName)} TO ${this.quote(newName)}
            `;

        sql = this.trigger(EventsEnum.AttributeUpdate, sql);

        try {
            await this.client.query(sql);
        } catch (e: any) {
            this.processException(e, `Failed to rename attribute '${oldName}' to '${newName}' in collection '${collection}'`);
        }
    }

    public async deleteAttribute(
        collection: string,
        name: string
    ): Promise<void> {
        if (!name || !collection) {
            throw new DatabaseException("Failed to delete attribute: name and collection are required");
        }

        const table = this.getSQLTable(collection);
        let sql = `
                ALTER TABLE ${table}
                DROP COLUMN ${this.quote(name)}
            `;

        sql = this.trigger(EventsEnum.AttributeDelete, sql);

        try {
            await this.client.query(sql);
        } catch (e: any) {
            this.processException(e, `Failed to delete attribute '${name}' from collection '${collection}'`);
        }
    }

    public async getSchemaAttributes(collection: string): Promise<Doc<ColumnInfo>[]> {
        const schema = this.$schema;
        const table = `${this.$namespace}_${this.sanitize(collection)}`;

        const sql = `
            SELECT
                cols.column_name AS "$id",
                pg_get_expr(def.adbin, def.adrelid) AS "columnDefault",
                cols.is_nullable AS "isNullable",
                cols.data_type AS "dataType",
                cols.character_maximum_length AS "characterMaximumLength",
                cols.numeric_precision AS "numericPrecision",
                cols.numeric_scale AS "numericScale",
                cols.datetime_precision AS "datetimePrecision",
                cols.udt_name AS "udtName",
                att.attidentity AS "identityFlag",
                CASE WHEN pk.constraint_type = 'PRIMARY KEY' THEN 'PRI' ELSE '' END AS "columnKey"
            FROM
                information_schema.columns AS cols
            JOIN
                pg_class AS cls ON cls.relname = $1
            JOIN
                pg_namespace AS ns ON ns.oid = cls.relnamespace AND ns.nspname = $2
            LEFT JOIN
                pg_attribute AS att ON att.attrelid = cls.oid AND att.attname = cols.column_name
            LEFT JOIN
                pg_attrdef AS def ON def.adrelid = cls.oid AND def.adnum = att.attnum
            LEFT JOIN (
                SELECT
                    kcu.column_name,
                    tc.constraint_type
                FROM
                    information_schema.table_constraints AS tc
                JOIN
                    information_schema.key_column_usage AS kcu
                    ON tc.constraint_name = kcu.constraint_name
                    AND tc.table_schema = kcu.table_schema
                    AND tc.table_name = kcu.table_name
                WHERE
                    tc.constraint_type = 'PRIMARY KEY'
                    AND tc.table_schema = $2
                    AND tc.table_name = $1
            ) AS pk ON pk.column_name = cols.column_name
            WHERE
                cols.table_schema = $2
                AND cols.table_name = $1
            ORDER BY
                cols.ordinal_position;
        `;

        try {
            const [rows]: any = await this.client.query(sql, [table, schema]);

            return rows.map((row: any) => {
                row.isNullable = row.isNullable === 'YES' ? 'YES' : 'NO';
                if (row.udtName?.startsWith('_')) {
                    row.dataType = row.udtName.slice(1) + '[]';
                }
                switch (row.dataType) {
                    case 'int4': row.dataType = 'integer'; break;
                    case 'int8': row.dataType = 'bigint'; break;
                    case 'float8': row.dataType = 'double precision'; break;
                    case 'bool': row.dataType = 'boolean'; break;
                    case 'timestamptz': row.dataType = 'timestamptz'; break;
                    case 'jsonb': row.dataType = 'json'; break;
                    case 'uuid': row.dataType = 'uuid'; break;
                    default: break;
                }
                row.extra = row.identityFlag === 'a' || row.identityFlag === 'd' ? 'auto_increment' : '';
                delete row.identityFlag;

                return Doc.from(row);
            });
        } catch (e: any) {
            this.processException(e, 'Failed to get schema attributes');
        }
    }

    public async createRelationship(
        collection: string,
        relatedCollection: string,
        type: RelationEnum,
        twoWay: boolean = false,
        id: string = '',
        twoWayKey: string = ''
    ): Promise<boolean> {
        const name = this.sanitize(collection);
        const relatedName = this.sanitize(relatedCollection);
        const table = this.getSQLTable(name);
        const relatedTable = this.getSQLTable(relatedName);
        const sanitizedId = this.sanitize(id);
        const sanitizedTwoWayKey = this.sanitize(twoWayKey);
        const sqlType = this.getSQLType(AttributeEnum.Relationship, 0, false);

        let sql: string;

        switch (type) {
            case RelationEnum.OneToOne:
                sql = `
                    ALTER TABLE ${table} 
                    ADD COLUMN ${this.quote(sanitizedId)} ${sqlType} DEFAULT NULL;
                `;

                if (twoWay) {
                    sql += `
                        ALTER TABLE ${relatedTable} 
                        ADD COLUMN ${this.quote(sanitizedTwoWayKey)} ${sqlType} DEFAULT NULL;
                    `;
                }
                break;

            case RelationEnum.OneToMany:
                sql = `
                    ALTER TABLE ${relatedTable} 
                    ADD COLUMN ${this.quote(sanitizedTwoWayKey)} ${sqlType} DEFAULT NULL;
                `;
                break;

            case RelationEnum.ManyToOne:
                sql = `
                    ALTER TABLE ${table} 
                    ADD COLUMN ${this.quote(sanitizedId)} ${sqlType} DEFAULT NULL;
                `;
                break;

            case RelationEnum.ManyToMany:
                return true;

            default:
                throw new DatabaseException('Invalid relationship type');
        }

        sql = this.trigger(EventsEnum.AttributeCreate, sql);

        try {
            await this.client.query(sql);
            return true;
        } catch (e: any) {
            this.processException(e, `Failed to create relationship between '${collection}' and '${relatedCollection}'`);
        }
    }

    public async updateRelationship(
        collection: string,
        relatedCollection: string,
        type: RelationEnum,
        twoWay: boolean = false,
        key: string,
        twoWayKey: string,
        side: RelationSideEnum,
        newKey?: string,
        newTwoWayKey?: string
    ): Promise<boolean> {
        const name = this.sanitize(collection);
        const relatedName = this.sanitize(relatedCollection);
        const table = this.getSQLTable(name);
        const relatedTable = this.getSQLTable(relatedName);
        const sanitizedKey = this.sanitize(key);
        const sanitizedTwoWayKey = this.sanitize(twoWayKey);

        let sql = '';

        if (newKey) {
            newKey = this.sanitize(newKey);
        }
        if (newTwoWayKey) {
            newTwoWayKey = this.sanitize(newTwoWayKey);
        }

        switch (type) {
            case RelationEnum.OneToOne:
                if (sanitizedKey !== newKey) {
                    sql = `ALTER TABLE ${table} RENAME COLUMN ${this.quote(sanitizedKey)} TO ${this.quote(newKey!)};`;
                }
                if (twoWay && sanitizedTwoWayKey !== newTwoWayKey) {
                    sql += `ALTER TABLE ${relatedTable} RENAME COLUMN ${this.quote(sanitizedTwoWayKey)} TO ${this.quote(newTwoWayKey!)};`;
                }
                break;
            case RelationEnum.OneToMany:
                if (side === RelationSideEnum.Parent) {
                    if (sanitizedTwoWayKey !== newTwoWayKey) {
                        sql = `ALTER TABLE ${relatedTable} RENAME COLUMN ${this.quote(sanitizedTwoWayKey)} TO ${this.quote(newTwoWayKey!)};`;
                    }
                } else {
                    if (sanitizedKey !== newKey) {
                        sql = `ALTER TABLE ${table} RENAME COLUMN ${this.quote(sanitizedKey)} TO ${this.quote(newKey!)};`;
                    }
                }
                break;
            case RelationEnum.ManyToOne:
                if (side === RelationSideEnum.Child) {
                    if (sanitizedTwoWayKey !== newTwoWayKey) {
                        sql = `ALTER TABLE ${relatedTable} RENAME COLUMN ${this.quote(sanitizedTwoWayKey)} TO ${this.quote(newTwoWayKey!)};`;
                    }
                } else {
                    if (sanitizedKey !== newKey) {
                        sql = `ALTER TABLE ${table} RENAME COLUMN ${this.quote(sanitizedKey)} TO ${this.quote(newKey!)};`;
                    }
                }
                break;
            case RelationEnum.ManyToMany:
                // TODO: 
                break;
            default:
                throw new DatabaseException('Invalid relationship type');
        }

        if (!sql) {
            return true;
        }

        sql = this.trigger(EventsEnum.AttributeUpdate, sql);

        try {
            await this.client.query(sql);
            return true;
        } catch (e: any) {
            this.processException(e, `Failed to update relationship between '${collection}' and '${relatedCollection}'`);
        }
    }

    public async deleteRelationship(
        collection: string,
        relatedCollection: string,
        type: RelationEnum,
        twoWay: boolean,
        key: string,
        twoWayKey: string,
        side: RelationSideEnum
    ): Promise<boolean> {
        const name = this.sanitize(collection);
        const relatedName = this.sanitize(relatedCollection);
        const table = this.getSQLTable(name);
        const relatedTable = this.getSQLTable(relatedName);
        const sanitizedKey = this.sanitize(key);
        const sanitizedTwoWayKey = this.sanitize(twoWayKey);

        let sql = '';

        switch (type) {
            case RelationEnum.OneToOne:
                if (side === RelationSideEnum.Parent) {
                    sql = `ALTER TABLE ${table} DROP COLUMN ${this.quote(sanitizedKey)};`;
                    if (twoWay) {
                        sql += `ALTER TABLE ${relatedTable} DROP COLUMN ${this.quote(sanitizedTwoWayKey)};`;
                    }
                } else if (side === RelationSideEnum.Child) {
                    sql = `ALTER TABLE ${relatedTable} DROP COLUMN ${this.quote(sanitizedTwoWayKey)};`;
                    if (twoWay) {
                        sql += `ALTER TABLE ${table} DROP COLUMN ${this.quote(sanitizedKey)};`;
                    }
                }
                break;
            case RelationEnum.OneToMany:
                if (side === RelationSideEnum.Parent) {
                    sql = `ALTER TABLE ${relatedTable} DROP COLUMN ${this.quote(sanitizedTwoWayKey)};`;
                } else {
                    sql = `ALTER TABLE ${table} DROP COLUMN ${this.quote(sanitizedKey)};`;
                }
                break;
            case RelationEnum.ManyToOne:
                if (side === RelationSideEnum.Child) {
                    sql = `ALTER TABLE ${relatedTable} DROP COLUMN ${this.quote(sanitizedTwoWayKey)};`;
                } else {
                    sql = `ALTER TABLE ${table} DROP COLUMN ${this.quote(sanitizedKey)};`;
                }
                break;
            case RelationEnum.ManyToMany:
                break;
            default:
                throw new DatabaseException('Invalid relationship type');
        }

        if (!sql) {
            return true;
        }

        sql = this.trigger(EventsEnum.AttributeDelete, sql);

        try {
            await this.client.query(sql);
            return true;
        } catch (e: any) {
            this.processException(e, `Failed to delete relationship between '${collection}' and '${relatedCollection}'`);
        }
    }

    public async updateAttribute(
        { collection, key: name, newName, array, size, type }: UpdateAttribute
    ): Promise<void> {
        const tableName = this.getSQLTable(this.sanitize(collection));
        const columnName = this.sanitize(name);
        const newColumnName = newName ? this.sanitize(newName) : null;
        const sqlType = this.getSQLType(type, size, array);

        let sql: string;
        if (newColumnName) {
            sql = `ALTER TABLE ${tableName} CHANGE COLUMN ${this.quote(columnName)} ${this.quote(newColumnName)} ${sqlType};`;
        } else {
            sql = `ALTER TABLE ${tableName} MODIFY ${this.quote(columnName)} ${sqlType};`;
        }

        sql = this.trigger(EventsEnum.AttributeUpdate, sql);

        try {
            await this.client.query(sql);
        } catch (e: any) {
            this.processException(e, 'Failed to update attribute');
        }
    }

    public async renameIndex(collectionId: string, oldName: string, newName: string): Promise<boolean> {
        const currentPgIndexName = `"${this.sanitize(this.getIndexName(collectionId, oldName))}"`;
        const newPgIndexName = `"${this.sanitize(this.getIndexName(collectionId, newName))}"`;

        let sql = `ALTER INDEX ${currentPgIndexName} RENAME TO ${newPgIndexName};`;
        sql = this.trigger(EventsEnum.IndexRename, sql);

        try {
            await this.client.query(sql);
            return true;
        } catch (e: any) {
            throw this.processException(e, `Failed to rename index from ${oldName} to ${newName} for collection ${collectionId}`);
        }
    }

    public async createIndex(
        { collection: collectionId, name, type, attributes, orders = [], attributeTypes = {} }: CreateIndex
    ): Promise<boolean> {
        const isUnique = type === IndexEnum.Unique;
        const isFulltext = type === IndexEnum.FullText;

        let usingClause = '';
        if (isFulltext) {
            usingClause = 'USING GIN';
        }

        const preparedAttributes = attributes.map((attrId, i) => {
            const collectionAttribute = attributeTypes[attrId.toLowerCase()];

            if (!collectionAttribute) {
                throw new DatabaseException(`Attribute '${attrId}' not found in collection metadata.`);
            }

            const internalKey = this.getInternalKeyForAttribute(attrId);
            const sanitizedKey = this.sanitize(internalKey);
            const pgKey = this.quote(sanitizedKey);

            if (isFulltext) {
                // Full-text search indexes on a `TSVECTOR` representation of the column.
                // We use the `to_tsvector` function for this.
                return `to_tsvector('english', ${pgKey})`;
            }

            if (collectionAttribute.array) {
                usingClause = 'USING GIN';
                return pgKey;
            }
            const order = (orders[i] && !isFulltext) ? ` ${orders[i]}` : '';
            return `${pgKey}${order}`;
        });

        if (isFulltext && preparedAttributes.length > 1) {
            const combinedTsvector = preparedAttributes.join(' || ');
            preparedAttributes.length = 0;
            preparedAttributes.push(combinedTsvector);
        }

        const pgTable = this.getSQLTable(collectionId);
        const pgIndexId = this.quote(this.getIndexName(collectionId, name));
        const uniqueClause = isUnique ? 'UNIQUE' : '';

        let attributesForSql = preparedAttributes.join(', ');

        if (this.$sharedTables && !isFulltext) {
            const pgTenantKey = `"${this.sanitize('_tenant')}"`;
            attributesForSql = `${pgTenantKey}, ${attributesForSql}`;
        }

        const sql = `CREATE ${uniqueClause} INDEX ${pgIndexId} ON ${pgTable} ${usingClause} (${attributesForSql})`;
        const finalSql = this.trigger(EventsEnum.IndexCreate, sql);

        try {
            await this.client.query(finalSql);
            return true;
        } catch (e) {
            throw this.processException(e);
        }
    }

    public async deleteIndex(collection: string, id: string): Promise<boolean> {
        const pgIndexName = this.quote(this.getIndexName(collection, id));

        let sql = `DROP INDEX IF EXISTS ${pgIndexName};`;
        sql = this.trigger(EventsEnum.IndexDelete, sql);

        try {
            await this.client.query(sql);
            return true;
        } catch (e: any) {
            this.processException(e, `Failed to delete index ${id} from collection ${collection}`);
        }
    }

    public async createDocument<D extends Doc>(collection: string, document: D): Promise<D> {
        try {
            const attributes: Record<string, any> = { ...document.getAll() };
            attributes['_createdAt'] = document.createdAt();
            attributes['_updatedAt'] = document.updatedAt();
            attributes['_permissions'] = document.getPermissions();

            if (this.$sharedTables) {
                attributes['_tenant'] = document.getTenant();
            }

            const name = this.sanitize(collection);
            const columns: string[] = [];
            const placeholders: string[] = [];
            const values: any[] = [];

            // Insert attributes
            Object.entries(attributes).forEach(([attribute, value], idx) => {
                if (this.$internalAttrs.includes(attribute)) return;
                const column = this.sanitize(attribute);
                columns.push(this.$.quote(column));
                placeholders.push('?');
                values.push(value);
            });

            // Insert internal ID if set
            if (document.getSequence()) {
                columns.push('_id');
                placeholders.push('?');
                values.push(document.getSequence());
            }

            columns.push('_uid');
            placeholders.push('?');
            values.push(document.getId());

            let sql = `
                INSERT INTO ${this.getSQLTable(name)} (${columns.join(', ')})
                VALUES (${placeholders.join(', ')}) RETURNING _id
            `;

            sql = this.trigger(EventsEnum.DocumentCreate, sql);
            console.log({ values })
            const { rows } = await this.client.query(sql, values);

            // Set $sequence from insertId
            document.set('$sequence', rows[0]['_id']);

            if (!rows[0]['_id']) {
                throw new DatabaseException('Error creating document empty "$sequence"');
            }

            const permissions: any[] = [];
            for (const type of Database.PERMISSIONS || []) {
                const perms = document.getPermissionsByType(type);
                if (perms && perms.length) {
                    const row: any[] = [type, perms, document.getSequence()];
                    if (this.$sharedTables) {
                        row.push(document.getTenant());
                    }
                    permissions.push(row);
                }
            }

            if (permissions.length) {
                const columnsPerm = ['_type', '_permissions', '_document'];
                if (this.$sharedTables) columnsPerm.push('_tenant');
                const placeholdersPerm = '(' + columnsPerm.map(() => '?').join(', ') + ')';
                const sqlPermissions = `
                    INSERT INTO ${this.getSQLTable(name + '_perms')} (${columnsPerm.join(', ')})
                    VALUES ${permissions.map(() => placeholdersPerm).join(', ')}
                `;
                const valuesPerm = permissions.flat();
                await this.client.query(sqlPermissions, valuesPerm);
            }

            return document;
        } catch (e: any) {
            throw this.processException(e, 'Failed to create document');
        }
    }

    public async updateDocument<D extends Doc>(collection: string, document: D, skipPermissions: boolean = false): Promise<D> {
        try {
            const attributes: Record<string, any> = { ...document.getAll() };
            attributes['_createdAt'] = document.createdAt();
            attributes['_updatedAt'] = document.updatedAt();
            attributes['_permissions'] = document.getPermissions();

            const name = this.sanitize(collection);
            let columns = '';

            let removePermissions: any = null;
            let addPermissions: any = null;

            if (!skipPermissions) {
                const perms = await this.updatePermissions(name, document);
                perms.addPermissions && (addPermissions = perms.addPermissions);
                perms.removePermissions && (removePermissions = perms.removePermissions);
            }

            // Update attributes
            const updateParams: any[] = [];
            const columnUpdates: string[] = [];

            for (const [attribute, value] of Object.entries(attributes)) {
                if (this.$internalAttrs.includes(attribute)) continue;

                const column = this.sanitize(attribute);
                columnUpdates.push(`${this.quote(column)} = ?`);
                updateParams.push(value);
            }

            columns = columnUpdates.join(', ');

            let sql = `
                    UPDATE ${this.getSQLTable(name)}
                    SET ${columns}, _uid = ?
                    WHERE _id = ?
                    ${this.getTenantQuery(collection)}
                `;

            sql = this.trigger(EventsEnum.DocumentUpdate, sql);

            updateParams.push(document.getId());
            updateParams.push(document.getSequence());
            if (this.$sharedTables) {
                updateParams.push(this.$tenantId);
            }

            await this.client.query(sql, updateParams);

            if (removePermissions) {
                await this.client.query(removePermissions.sql, removePermissions.params);
            }
            if (addPermissions) {
                await this.client.query(addPermissions.sql, addPermissions.params);
            }
        } catch (e: any) {
            throw this.processException(e, 'Failed to update document');
        }

        return document;
    }

    /**
     * Generates an upsert (insert or update) SQL statement for batch operations.
     * If `attribute` is provided, it will increment that column on duplicate key.
     */
    public getUpsertStatement(
        tableName: string,
        columns: string,
        batchKeys: string[],
        attributes: Record<string, any>,
        attribute: string = ''
    ): string {
        const getUpdateClause = (attribute: string, increment = false): string => {
            const quotedAttr = this.quote(this.sanitize(attribute));
            let newValue: string;
            if (increment) {
                newValue = `${quotedAttr} + VALUES(${quotedAttr})`;
            } else {
                newValue = `VALUES(${quotedAttr})`;
            }
            if (this.$sharedTables) {
                return `${quotedAttr} = IF(_tenant = VALUES(_tenant), ${newValue}, ${quotedAttr})`;
            }
            return `${quotedAttr} = ${newValue}`;
        };

        let updateColumns: string[];
        if (attribute) {
            // Increment specific column by its new value in place
            updateColumns = [
                getUpdateClause(attribute, true),
                getUpdateClause('_updatedAt')
            ];
        } else {
            // Update all columns
            updateColumns = Object.keys(attributes).map(attr => getUpdateClause(this.sanitize(attr)));
        }

        const sql = `
                INSERT INTO ${this.getSQLTable(tableName)} ${columns}
                VALUES ${batchKeys.join(', ')}
                ON DUPLICATE KEY UPDATE
                    ${updateColumns.join(', ')}
            `;

        return sql;
    }

    public async findWithRelations(collection: string, query: ProcessedQuery, options: {
        forUpdate?: boolean;
    } = {}): Promise<Record<string, any>[]> {
        const sqlResult = this.buildSql(query, options);
        console.log('Deep Find SQL:', sqlResult.sql, sqlResult.params);

        try {
            const { rows } = await this.client.query(sqlResult.sql, sqlResult.params);
            console.log({ rows })
            return rows;
        } catch (e: any) {
            throw this.processException(e, `Failed to execute deep find query for collection '${collection}'`);
        }
    }

    /**
     * Builds a comprehensive SQL query with joins and filters for n-level relationships
     */
    protected buildSql(query: ProcessedQuery, extra: {
        forUpdate?: boolean;
    } = {}): {
        sql: string;
        params: any[];
        joins: string[];
        selections: string[];
    } {
        const { selections, populateQueries = [], filters, collection, ...options } = query;
        const mainTableAlias = 'main';
        const collectionName = this.sanitize(collection.getId());
        const mainTable = this.getSQLTable(collectionName);

        const result = this.handleConditions({
            populateQueries,
            tableAlias: mainTableAlias,
            depth: 0,
            collection,
            filters,
            selections,
            ...options,
        })
        let orderSql = '';

        if (result.orders.length) {
            orderSql = `ORDER BY ${result.orders.join(', ')}`
        }

        const limitClause = options.limit ? `LIMIT ?` : '';
        if (options.limit) result.params.push(options.limit);

        const offsetClause = options.offset ? `OFFSET ?` : '';
        if (options.offset) result.params.push(options.offset);

        // Build cursor conditions if provided
        const cursorConditions = this.buildCursorConditions(options.cursor as any, options.cursorDirection as any, options.orderAttributes || [], mainTableAlias);
        if (cursorConditions.condition) {
            result.conditions.push(cursorConditions.condition);
            result.params.push(...cursorConditions.params);
        }

        const finalWhereClause = result.conditions.length > 0 ? `WHERE ${result.conditions.join(' AND ')}` : '';
        const sql = `
            SELECT DISTINCT ${result.selectionsSql.join(', ')}
            FROM ${mainTable} AS ${this.quote(mainTableAlias)}
            ${result.joins.join(' ')}
            ${finalWhereClause}
           ${orderSql}
            ${limitClause}
            ${offsetClause}
        `.trim().replace(/\s+/g, ' ');

        return {
            sql,
            selections: result.selectionsSql,
            params: result.params,
            joins: result.joins,
        };
    }


    /**
     * Recursively handles building selections, joins, where conditions, and order clauses for main and populated queries.
     */
    private handleConditions(
        {
            populateQueries = [],
            tableAlias,
            depth = 0,
            ...rest
        }: (ProcessedQuery | PopulateQuery) & { tableAlias?: string, depth: number }
    ) {
        const conditions: string[] = [];
        const selectionsSql: string[] = [];
        const joins: string[] = [];
        let orders: string[] = [];
        const params: any[] = [];
        tableAlias = tableAlias ?? 'main';

        const { collection, filters = [], selections = [], orderAttributes = [], orderTypes = [], skipAuth } = rest;

        // Build selections for the current table
        selectionsSql.push(...this.buildSelections(selections, tableAlias, collection));

        // Build WHERE conditions for the current table
        const whereInfo = this.buildWhereConditions(filters, tableAlias, []);
        if (whereInfo.conditions.length) {
            conditions.push(...whereInfo.conditions);
            params.push(...whereInfo.params);
        }

        if (Authorization.getStatus() && collection.get('documentSecurity', false) && tableAlias === 'main') {
            const roles = Authorization.getRoles();
            conditions.push(this.getSQLPermissionsCondition({
                collection: collection.getId(), roles, alias: tableAlias, type: PermissionEnum.Read
            }));
            if (this.$sharedTables) params.push(this.$tenantId);
        }

        if (this.$sharedTables) {
            params.push(this.$tenantId);
            conditions.push(this.getTenantQuery(collection.getId(), tableAlias, undefined, ''));
        }

        // Build ORDER BY clause for the current table
        const _orders = this.buildOrderClause(orderAttributes, orderTypes, tableAlias);
        if (_orders.length) {
            orders.push(..._orders);
        }

        // Recursively handle populated queries (relationships)
        for (let i = 0; i < populateQueries.length; i++) {
            const populateQuery: PopulateQuery = populateQueries[i]!;
            const { attribute, authorized, ...rest } = populateQuery;
            if (!authorized) continue;
            const relationshipAttr = collection.get('attributes', [])
                .find((attr) => attr.get('type') === AttributeEnum.Relationship && attr.get('key', attr.getId()) === attribute);

            if (!relationshipAttr) continue;

            const relationAlias = `rel_${depth}_${i}`;
            const parentAlias = tableAlias;
            const options = relationshipAttr.get('options', {}) as RelationOptions;
            const side = options.side;
            const relationType = options.relationType;
            const twoWayKey = options.twoWayKey;
            const relationshipKey = relationshipAttr.get('key', relationshipAttr.getId());

            const relatedTableName = this.sanitize(options.relatedCollection);
            const relatedTable = this.getSQLTable(relatedTableName);

            const joinCondition = this.buildJoinCondition(
                relationType,
                parentAlias,
                relationAlias,
                relationshipKey,
                twoWayKey,
                side
            );

            if (joinCondition) {
                joins.push(`LEFT JOIN ${relatedTable} AS ${this.quote(relationAlias)} ON ${joinCondition}`);

                // Add permissions check for the joined table if shared tables are enabled
                if (Authorization.getStatus() && rest.collection.get('documentSecurity', false)) {
                    const roles = Authorization.getRoles();
                    joins.push(`AND ${this.getSQLPermissionsCondition({
                        collection: rest.collection.getId(), roles, alias: relationAlias, type: PermissionEnum.Read
                    })}`);
                    if (this.$sharedTables) params.push(this.$tenantId);
                }

                if (this.$sharedTables) {
                    joins.push(`AND (${this.quote(relationAlias)}.${this.quote('_tenant')} = ? OR ${this.quote(relationAlias)}.${this.quote('_tenant')} IS NULL)`);
                    params.push(this.$tenantId);
                }
            }

            // Recursively handle nested population
            const nestedResult = this.handleConditions({
                attribute,
                ...rest,
                depth: depth + 1,
                tableAlias: relationAlias
            });

            // Prefix the selections to avoid conflicts
            const prefixedSelections = nestedResult.selectionsSql.map(sel => {
                const parts = sel.split(' AS ');
                const prefix = (side === RelationSideEnum.Child && options.twoWay) ? twoWayKey : relationshipKey;
                if (parts.length === 2 && parts[1]) {
                    return `${parts[0]} AS ${this.quote(`${prefix}_${parts[1].replace(/"/g, '')}`)}`;
                }
                return sel;
            });

            // Merge results
            if (nestedResult.conditions.length) conditions.push(...nestedResult.conditions);
            if (nestedResult.joins.length) joins.push(...nestedResult.joins);
            if (prefixedSelections.length) selectionsSql.push(...prefixedSelections);
            if (nestedResult.orders.length) orders.push(...nestedResult.orders);
            if (nestedResult.params.length) params.push(...nestedResult.params);
        }

        return {
            conditions,
            selectionsSql,
            orders,
            params,
            joins
        };
    }

    /**
     * Builds selection clauses for the main table and relationship
     */
    private buildSelections(selections: string[], tableAlias: string, collection: Doc<Collection>): string[] {
        const result: string[] = [];
        const attributes = collection.get('attributes', []);

        // If no specific selections, include all non-relationship attributes
        const fieldsToSelect = selections.length > 0 ? selections :
            attributes
                .filter((attr) => attr.get('type') !== AttributeEnum.Relationship && attr.get('type') !== AttributeEnum.Virtual)
                .map((attr) => attr.get('key', attr.getId()));

        const internalFields = ['$id', '$sequence', '$createdAt', '$updatedAt', '$permissions'];
        const allFields = [...new Set([...internalFields, ...fieldsToSelect])];

        // result.push(`'${collection.getId()}' AS ${this.quote('$collection')}`);
        for (const field of allFields) {
            const dbKey = this.getInternalKeyForAttribute(field);
            const sanitizedKey = this.sanitize(dbKey);
            result.push(`${this.quote(tableAlias)}.${this.quote(sanitizedKey)} AS ${this.quote(field)}`);
        }

        if (this.$sharedTables) {
            result.push(`${this.quote(tableAlias)}.${this.quote('_tenant')} AS ${this.quote('$tenant')}`);
        }

        return result;
    }

    /**
     * Builds JOIN condition based on relationship type
     */
    private buildJoinCondition(
        relationType: RelationEnum,
        parentAlias: string,
        relationAlias: string,
        relationshipKey: string,
        twoWayKey: string = '',
        side: RelationSideEnum
    ): string | null {
        const parentUidCol = `${this.quote(parentAlias)}.${this.quote('_uid')}`;
        const relationUidCol = `${this.quote(relationAlias)}.${this.quote('_uid')}`;
        const parentRelCol = `${this.quote(parentAlias)}.${this.quote(this.sanitize(relationshipKey))}`;
        const relationRelCol = `${this.quote(relationAlias)}.${this.quote(this.sanitize(twoWayKey))}`;

        switch (relationType) {
            case RelationEnum.OneToOne:
                if (side === RelationSideEnum.Parent) {
                    return `${parentRelCol} = ${relationUidCol}`;
                } else {
                    return `${parentUidCol} = ${relationRelCol}`;
                }

            case RelationEnum.OneToMany:
                if (side === RelationSideEnum.Parent) {
                    return `${parentUidCol} = ${relationRelCol}`;
                } else {
                    return `${parentRelCol} = ${relationUidCol}`;
                }

            case RelationEnum.ManyToOne:
                if (side === RelationSideEnum.Child) {
                    return `${parentUidCol} = ${relationRelCol}`;
                } else {
                    return `${parentRelCol} = ${relationUidCol}`;
                }

            case RelationEnum.ManyToMany:
                // For ManyToMany, we would need a junction table
                // This is a simplified implementation
                // return `${parentUidCol} = ${relationUidCol}`;
                throw new Error('NOT IMPLEMENTED')

            default:
                return null;
        }
    }

    /**
     * Builds WHERE conditions from queries
     */
    private buildWhereConditions(
        queries: Query[],
        tableAlias: string,
        params: any[],
    ): { conditions: string[]; params: any[] } {
        const conditions: string[] = [];
        const conditionParams: any[] = [];

        // Add basic tenant filtering for shared tables
        if (this.$sharedTables) {
            conditions.push(`(${this.quote(tableAlias)}.${this.quote('_tenant')} = ? OR ${this.quote(tableAlias)}.${this.quote('_tenant')} IS NULL)`);
            conditionParams.push(this.$tenantId);
        }

        // Process query filters
        for (const query of queries) {
            const condition = this.buildQueryCondition(query, tableAlias);
            if (condition.sql) {
                conditions.push(condition.sql);
                conditionParams.push(...condition.params);
            }
        }

        console.log({ conditions, conditionParams })
        return { conditions, params: conditionParams };
    }

    /**
     * Builds a single query condition
     */
    private buildQueryCondition(query: Query, tableAlias: string): { sql: string; params: any[] } {
        const method = query.getMethod();
        const attribute = query.getAttribute();
        const values = query.getValues();
        const params: any[] = [];

        if (method === QueryType.Select || method === QueryType.Populate) {
            return { sql: '', params: [] };
        }

        const dbKey = this.getInternalKeyForAttribute(attribute);
        const sanitizedKey = this.sanitize(dbKey);
        const columnRef = `${this.quote(tableAlias)}.${this.quote(sanitizedKey)}`;

        let sql = '';

        switch (method) {
            case QueryType.Equal:
                if (values.length === 1) {
                    sql = `${columnRef} = ?`;
                    params.push(values[0]);
                } else {
                    sql = `${columnRef} IN (${values.map(() => '?').join(', ')})`;
                    params.push(...values);
                }
                break;

            case QueryType.NotEqual:
                if (values.length === 1) {
                    sql = `${columnRef} != ?`;
                    params.push(values[0]);
                } else {
                    sql = `${columnRef} NOT IN (${values.map(() => '?').join(', ')})`;
                    params.push(...values);
                }
                break;

            case QueryType.LessThan:
                sql = `${columnRef} < ?`;
                params.push(values[0]);
                break;

            case QueryType.LessThanEqual:
                sql = `${columnRef} <= ?`;
                params.push(values[0]);
                break;

            case QueryType.GreaterThan:
                sql = `${columnRef} > ?`;
                params.push(values[0]);
                break;

            case QueryType.GreaterThanEqual:
                sql = `${columnRef} >= ?`;
                params.push(values[0]);
                break;

            case QueryType.Contains:
                if (query.onArray()) {
                    sql = `${columnRef} @> ?::jsonb`;
                    params.push(JSON.stringify(values));
                } else {
                    sql = `${columnRef} LIKE ?`;
                    params.push(`%${values[0]}%`);
                }
                break;

            case QueryType.StartsWith:
                sql = `${columnRef} LIKE ?`;
                params.push(`${values[0]}%`);
                break;

            case QueryType.EndsWith:
                sql = `${columnRef} LIKE ?`;
                params.push(`%${values[0]}`);
                break;

            case QueryType.IsNull:
                sql = `${columnRef} IS NULL`;
                break;

            case QueryType.IsNotNull:
                sql = `${columnRef} IS NOT NULL`;
                break;

            case QueryType.Between:
                sql = `${columnRef} BETWEEN ? AND ?`;
                params.push(values[0], values[1]);
                break;

            case QueryType.Search:
                sql = `to_tsvector('english', ${columnRef}) @@ plainto_tsquery('english', ?)`;
                params.push(values[0]);
                break;

            case QueryType.And:
                const andConditions = (values as Query[]).map(subQuery =>
                    this.buildQueryCondition(subQuery, tableAlias)
                );
                sql = `(${andConditions.map(c => c.sql).filter(Boolean).join(' AND ')})`;
                andConditions.forEach(c => params.push(...c.params));
                break;

            case QueryType.Or:
                const orConditions = (values as Query[]).map(subQuery =>
                    this.buildQueryCondition(subQuery, tableAlias)
                );
                sql = `(${orConditions.map(c => c.sql).filter(Boolean).join(' OR ')})`;
                orConditions.forEach(c => params.push(...c.params));
                break;

            default:
                // Handle other query types as needed
                break;
        }

        return { sql, params };
    }

    /**
     * Builds ORDER BY clause
     */
    private buildOrderClause(
        orderAttributes: string[],
        orderTypes: string[],
        tableAlias: string
    ): string[] {
        if (orderAttributes.length === 0) {
            // Default order by _id
            return [`${this.quote(tableAlias)}.${this.quote('_id')} ASC`];
        }

        const orderParts = orderAttributes.map((attr, index) => {
            const dbKey = this.getInternalKeyForAttribute(attr);
            const sanitizedKey = this.sanitize(dbKey);
            const orderType = orderTypes[index] || 'ASC';
            return `${this.quote(tableAlias)}.${this.quote(sanitizedKey)} ${orderType}`;
        });

        return orderParts;
    }

    /**
     * Builds cursor conditions for pagination
     */
    private buildCursorConditions(
        cursor: Record<string, any> = {},
        cursorDirection: string = 'AFTER',
        orderAttributes: string[],
        tableAlias: string
    ): { condition: string; params: any[] } {
        if (!cursor || Object.keys(cursor).length === 0 || orderAttributes.length === 0) {
            return { condition: '', params: [] };
        }

        const conditions: string[] = [];
        const params: any[] = [];
        const operator = cursorDirection === 'AFTER' ? '>' : '<';

        // Build cursor condition for pagination
        if (orderAttributes.length === 1 && orderAttributes[0] === '$sequence') {
            // Simple case: single unique attribute
            const attr = orderAttributes[0];
            const dbKey = this.getInternalKeyForAttribute(attr);
            const sanitizedKey = this.sanitize(dbKey);
            conditions.push(`${this.quote(tableAlias)}.${this.quote(sanitizedKey)} ${operator} ?`);
            params.push(cursor[attr]);
        } else {
            // Complex case: multiple attributes (tie-breaking)
            for (let i = 0; i < orderAttributes.length; i++) {
                const attr = orderAttributes[i];
                if (!attr) continue;
                const dbKey = this.getInternalKeyForAttribute(attr);
                const sanitizedKey = this.sanitize(dbKey);

                const equalityConditions = orderAttributes
                    .slice(0, i)
                    .filter((prevAttr): prevAttr is string => prevAttr !== undefined)
                    .map(prevAttr => {
                        const prevDbKey = this.getInternalKeyForAttribute(prevAttr);
                        const prevSanitizedKey = this.sanitize(prevDbKey);
                        params.push(cursor[prevAttr]);
                        return `${this.quote(tableAlias)}.${this.quote(prevSanitizedKey)} = ?`;
                    });

                equalityConditions.push(`${this.quote(tableAlias)}.${this.quote(sanitizedKey)} ${operator} ?`);
                params.push(cursor[attr]);

                conditions.push(`(${equalityConditions.join(' AND ')})`);
            }
        }

        return {
            condition: conditions.length > 0 ? `(${conditions.join(' OR ')})` : '',
            params
        };
    }
}
