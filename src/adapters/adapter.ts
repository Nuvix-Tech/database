import { Client, PoolOptions } from "pg";
import { BaseAdapter } from "./base.js";
import { PostgresClient } from "./postgres.js";
import { AttributeEnum, EventsEnum, IndexEnum, RelationEnum, RelationSideEnum } from "@core/enums.js";
import { Query, QueryType } from "@core/query.js";
import { CreateCollectionOptions } from "./interface.js";
import { DatabaseException } from "@errors/base.js";
import { Database } from "@core/database.js";
import { Doc } from "@core/doc.js";
import { NotFoundException } from "@errors/index.js";
import { Attribute } from "@validators/schema.js";
import { ColumnInfo, CreateAttribute, CreateIndex, CreateRelationship, UpdateAttribute } from "./types.js";

export class Adapter extends BaseAdapter {
    protected client: PostgresClient;

    constructor(client: PoolOptions | Client) {
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

        indexes?.forEach((index) => {
            const indexId = this.sanitize(index.getId());
            const indexType = index.get('type');
            let indexAttributes = index.get('attributes') as string[];
            const orders = index.get('orders') || [];

            const formattedIndexAttributes = indexAttributes.map((attributeKey, nested) => {
                const quotedAttribute = this.quote(this.getInternalKeyForAttribute(attributeKey));
                const order = orders[nested] ? ` ${orders[nested]}` : '';
                return `${quotedAttribute}${order}`;
            });

            let attributesSql = formattedIndexAttributes.join(', ');

            if (this.$sharedTables && indexType !== IndexEnum.FullText) {
                attributesSql = `"_tenant", ${attributesSql}`;
            }

            if (indexType === IndexEnum.FullText) {
                indexSql.push(`CREATE INDEX ${this.quote(indexId)} ON ${this.quote(name)} USING GIN (${attributesSql})`);
            } else {
                const uniqueClause = indexType === IndexEnum.Unique ? 'UNIQUE ' : '';
                indexSql.push(`CREATE ${uniqueClause}INDEX ${this.quote(indexId)} ON ${this.quote(name)} (${attributesSql})`);
            }
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
        const quotedTableName = this.quote(name);
        const tenantCol = this.quote('_tenant');

        if (this.$sharedTables) {
            mainTableColumns.splice(1, 0, `${tenantCol} BIGINT DEFAULT NULL`);
            primaryKeyDefinition = `PRIMARY KEY ("_id", ${tenantCol})`;
        } else {
            primaryKeyDefinition = `PRIMARY KEY ("_id")`;
        }

        const columnsAndConstraints = mainTableColumns.join(',\n');
        let tableSql = `
            CREATE TABLE ${quotedTableName} (
                ${columnsAndConstraints},
                ${primaryKeyDefinition}
            );
        `;

        const postTableIndexes: string[] = [];
        if (this.$sharedTables) {
            postTableIndexes.push(`CREATE UNIQUE INDEX "${name}_uid_tenant" ON ${quotedTableName} ("_uid", ${tenantCol});`);
            postTableIndexes.push(`CREATE INDEX "${name}_created_at_tenant" ON ${quotedTableName} (${tenantCol}, "_createdAt");`);
            postTableIndexes.push(`CREATE INDEX "${name}_updated_at_tenant" ON ${quotedTableName} (${tenantCol}, "_updatedAt");`);
            postTableIndexes.push(`CREATE INDEX "${name}_tenant_id" ON ${quotedTableName} (${tenantCol}, "_id");`);
        } else {
            postTableIndexes.push(`CREATE UNIQUE INDEX "${name}_uid" ON ${quotedTableName} ("_uid");`);
            postTableIndexes.push(`CREATE INDEX "${name}_created_at" ON ${quotedTableName} ("_createdAt");`);
            postTableIndexes.push(`CREATE INDEX "${name}_updated_at" ON ${quotedTableName} ("_updatedAt");`);
        }
        postTableIndexes.push(`CREATE INDEX "${name}_permissions_gin_idx" ON ${quotedTableName} USING GIN ("_permissions");`);

        tableSql = this.trigger(EventsEnum.CollectionCreate, tableSql);

        const permissionsTableName = this.quote(name + '_perms');
        const mainTableName = this.quote(name);

        const permissionsTableColumns = [
            `"_id" BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY`,
            `"_type" VARCHAR(12) NOT NULL`,
            `"_permissions" TEXT[] NOT NULL DEFAULT '{}'`,
            `"_document" BIGINT NOT NULL`,
            `FOREIGN KEY ("_document") REFERENCES ${mainTableName}("_id") ON DELETE CASCADE`
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
        await this.$client.transaction(async (client) => {
            await client.query(tableSql);
            for (const sql of postTableIndexes) {
                await client.query(sql);
            }

            for (const sql of indexSql) {
                await client.query(sql);
            }

            await client.query(permissionsTable);
            for (const sql of postPermissionsTableIndexes) {
                await client.query(sql);
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
        { name, collection, size, array, type }: CreateAttribute
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
            if (!attr.name || !attr.type) {
                throw new DatabaseException("Failed to create attribute: name and type are required");
            }

            const sqlType = this.getSQLType(attr.type, attr.size, attr.array);
            parts.push(`${this.quote(attr.name)} ${sqlType}`);
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
        relationship: CreateRelationship
    ): Promise<void> {
        const {
            collection, type, twoWay, target,
            junctionCollection, attribute
        } = relationship;

        if (!collection || !attribute || !type || !target || !target.collection) {
            throw new DatabaseException("Failed to create relationship: collection, attribute, type, and target are required");
        }

        const sanitizedCollection = this.sanitize(collection);
        const sanitizedRelatedCollection = this.sanitize(target.collection);
        const fkSqlType = this.getSQLType(AttributeEnum.Relationship, 0, false);

        const parts: string[] = [];

        switch (type) {
            case RelationEnum.ManyToOne:
            case RelationEnum.OneToOne: {
                const tableName = this.getSQLTable(sanitizedCollection);
                const attributeName = this.quote(this.getInternalKeyForAttribute(attribute));

                parts.push(`
                    ALTER TABLE ${tableName}
                    ADD COLUMN ${attributeName} ${fkSqlType} DEFAULT NULL;
                `);

                if (type === RelationEnum.OneToOne && twoWay) {
                    if (!target.attribute) {
                        throw new DatabaseException("Target attribute is required for a two-way, one-to-one relationship");
                    }
                    const relatedTableName = this.getSQLTable(sanitizedRelatedCollection);
                    const targetAttributeName = this.quote(this.getInternalKeyForAttribute(target.attribute));

                    parts.push(`
                        ALTER TABLE ${relatedTableName}
                        ADD COLUMN ${targetAttributeName} ${fkSqlType} DEFAULT NULL;
                    `);
                }
                break;
            }

            case RelationEnum.OneToMany: {
                if (!target.attribute) {
                    throw new DatabaseException("Target attribute is required for a one-to-many relationship");
                }

                const relatedTableName = this.getSQLTable(sanitizedRelatedCollection);
                const targetAttributeName = this.quote(this.getInternalKeyForAttribute(target.attribute));

                parts.push(`
                    ALTER TABLE ${relatedTableName}
                    ADD COLUMN ${targetAttributeName} ${fkSqlType} DEFAULT NULL;
                `);
                break;
            }

            case RelationEnum.ManyToMany: {
                if (!junctionCollection) {
                    throw new DatabaseException("Junction collection is required for many-to-many relationships");
                }
                if (!target.attribute) {
                    throw new DatabaseException("Target attribute is required for a many-to-many relationship");
                }

                const junctionCollectionName = this.getSQLTable(this.sanitize(junctionCollection));
                const collectionFk = this.quote(this.getInternalKeyForAttribute(sanitizedCollection));
                const targetFk = this.quote(this.getInternalKeyForAttribute(sanitizedRelatedCollection));
                const tenantCol = this.quote('_tenant');

                parts.push(`
                    CREATE TABLE ${junctionCollectionName} (
                        ${this.$sharedTables ? `${tenantCol} BIGINT DEFAULT NULL,` : ''}
                        ${collectionFk} ${fkSqlType} NOT NULL,
                        ${targetFk} ${fkSqlType} NOT NULL,
                        PRIMARY KEY (${tenantCol ? `${tenantCol}, ` : ''}${collectionFk}, ${targetFk})
                    );
                `);
                break;
            }
        }

        try {
            await this.client.transaction(async (client) => {
                for (const sql of parts) {
                    const finalSql = this.trigger(EventsEnum.AttributeCreate, sql);
                    await client.query(finalSql);
                }
            })
        } catch (e: any) {
            this.processException(e, `Failed to create relationship '${attribute}' for collection '${collection}'`);
        }
    }

    public async updateAttribute(
        { collection, name, newName, array, size, type }: UpdateAttribute
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

    public async renameIndex(collection: string, oldName: string, newName: string): Promise<boolean> {
        collection = this.sanitize(collection);
        oldName = this.sanitize(oldName);
        newName = this.sanitize(newName);

        let sql = `ALTER TABLE ${this.getSQLTable(collection)} RENAME INDEX \`${oldName}\` TO \`${newName}\`;`;
        sql = this.trigger(EventsEnum.IndexRename, sql);

        try {
            await this.client.query(sql);
            return true;
        } catch (e: any) {
            this.processException(e, `Failed to rename index from ${oldName} to ${newName}`);
        }
    }

    public async createIndex(
        { collection: c, name, type, attributes, orders = [], lengths = [], attributeTypes = [] }: CreateIndex
    ): Promise<boolean> {
        const collection = await this.getDocument(Database.METADATA, c);
        if (collection.empty()) {
            throw new NotFoundException(`Collection "${c}" not found.`)
        }

        const _attrs = collection.get('attributes', []);
        const collectionAttributes: Attribute[] = typeof _attrs === 'string' ? JSON.parse(_attrs) : _attrs;

        const indexAttributes = attributes.map((attr, i) => {
            let attribute: Attribute | undefined = collectionAttributes.find(
                (collectionAttribute) =>
                    String(collectionAttribute['$id']).toLowerCase() === String(attr).toLowerCase()
            );

            const order = !orders[i] || type === IndexEnum.FullText ? '' : orders[i];
            const length = !lengths[i] ? '' : `(${Number(lengths[i])})`;

            let internalAttr = this.sanitize(this.getInternalKeyForAttribute(attr));

            let attrSql = `\`${internalAttr}\`${length}${order ? ' ' + order : ''}`;

            if (this.$supportForCastIndexArray && attribute?.array) {
                attrSql = `(CAST(\`${internalAttr}\` AS char(${Database.ARRAY_INDEX_LENGTH}) ARRAY))`;
            }

            return attrSql;
        });

        let sqlType: string;
        switch (type) {
            case IndexEnum.Key:
                sqlType = 'INDEX';
                break;
            case IndexEnum.Unique:
                sqlType = 'UNIQUE INDEX';
                break;
            case IndexEnum.FullText:
                sqlType = 'FULLTEXT INDEX';
                break;
            default:
                throw new DatabaseException(`Unknown index type: ${attributeTypes?.[0]}. Must be one of ${IndexEnum.Key}, ${IndexEnum.Unique}, ${IndexEnum.FullText}`);
        }

        let attributesSql = indexAttributes.join(', ');
        if (this.$sharedTables && type !== IndexEnum.FullText) {
            attributesSql = `_tenant, ${attributesSql}`;
        }

        let sql = `CREATE ${sqlType} \`${this.sanitize(name)}\` ON ${this.getSQLTable(this.sanitize(collection.getId()))} (${attributesSql})`;
        sql = this.trigger(EventsEnum.IndexCreate, sql);

        try {
            await this.client.query(sql);
            return true;
        } catch (e: any) {
            this.processException(e, 'Failed to create index');
        }
    }

    public async deleteIndex(collection: string, id: string): Promise<boolean> {
        collection = this.sanitize(collection);
        id = this.sanitize(id);

        let sql = `ALTER TABLE ${this.getSQLTable(collection)} DROP INDEX \`${id}\`;`;
        sql = this.trigger(EventsEnum.IndexDelete, sql);

        try {
            await this.client.query(sql);
            return true;
        } catch (e: any) {
            // MariaDB error code 1091: Can't DROP 'index'; check that column/key exists
            if (e.code === "ER_CANT_DROP_FIELD_OR_KEY" || (e.errno === 1091)) {
                return true;
            }
            this.processException(e, `Failed to delete index ${id} from collection ${collection}`);
        }
    }

    public async createDocument<D extends Doc>(collection: string, document: D): Promise<D> {
        try {
            const attributes: Record<string, any> = { ...document.getAll() };
            attributes['_createdAt'] = document.createdAt();
            attributes['_updatedAt'] = document.updatedAt();
            attributes['_permissions'] = JSON.stringify(document.getPermissions());

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
                // Convert arrays to JSON, booleans to int
                if (Array.isArray(value) || (typeof value === "object" && value !== null)) {
                    value = JSON.stringify(value);
                }
                if (typeof value === 'boolean') {
                    value = value ? 1 : 0;
                }
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
                        VALUES (${placeholders.join(', ')})
                    `;

            sql = this.trigger(EventsEnum.DocumentCreate, sql);

            const [result]: any = await this.client.query(sql, values);

            // Set $sequence from insertId
            document.set('$sequence', result?.insertId);

            if (!result?.insertId) {
                throw new DatabaseException('Error creating document empty "$sequence"');
            }

            // Insert permissions
            const permissions: any[] = [];
            for (const type of Database.PERMISSIONS || []) {
                for (const permission of document.getPermissionsByType(type)) {
                    const row: any[] = [type, String(permission).replace(/"/g, ''), document.getId()];
                    if (this.$sharedTables) {
                        row.push(document.getTenant());
                    }
                    permissions.push(row);
                }
            }

            if (permissions.length) {
                const columnsPerm = ['_type', '_permission', '_document'];
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
            attributes['_permissions'] = JSON.stringify(document.getPermissions());

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

                let processedValue = value;
                if (Array.isArray(value) || (typeof value === "object" && value !== null)) {
                    processedValue = JSON.stringify(value);
                }
                if (typeof value === 'boolean') {
                    processedValue = value ? 1 : 0;
                }
                updateParams.push(processedValue);
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



}
