import { DatabaseException, NotFoundException, TransactionException } from "@errors/index.js";
import { BaseAdapter } from "./base.js";
import { CreateCollectionOptions, IAdapter, IClient } from "./interface.js";
import { createPool, Pool, Connection, PoolOptions, PoolConnection } from 'mysql2/promise';
import { AttributeEnum, EventsEnum, IndexEnum, RelationEnum, RelationSideEnum } from "@core/enums.js";
import { Database } from "@core/database.js";
import { Doc } from "@core/doc.js";
import { ColumnInfo, CreateIndex, UpdateAttribute } from "./types.js";
import { Attribute } from "@validators/schema.js";

class MariaClient implements IClient {
    private connection: Connection | Pool;
    private _type: 'connection' | 'pool' = 'connection';

    get $database(): string {
        return this.connection.config.database || '';
    }

    get $client() {
        return this.connection;
    }

    get $type(): 'connection' | 'pool' {
        return this._type;
    }

    constructor(options: PoolOptions | Connection) {
        if ('threadId' in options) {
            this.connection = options;
            this._type = 'connection';
        } else {
            this.connection = createPool(options);
            this._type = 'pool';
        }
    }

    async connect(): Promise<void> { }

    async disconnect(): Promise<void> {
        await this.connection.end()
    }

    get query() {
        return this.connection.query
    }

    quote(name: string): string {
        return `'${name}'`;
    }

    async ping(): Promise<void> {
        try {
            await this.$client.ping()
        } catch (e) {
            throw new DatabaseException(`Ping failed.`)
        }
    }

    async transaction<T>(callback: (client: Connection | PoolConnection) => Promise<T>): Promise<T> {
        const client = this._type === 'connection' ? this.connection : await (this.connection as Pool).getConnection()
        try {
            for (let attempts = 0; attempts < 3; attempts++) {
                try {
                    await client.beginTransaction()
                    const result = await callback(client)
                    await client.commit();
                    return result;
                } catch (action) {
                    try {
                        await client.rollback();
                    } catch (rollback) {
                        if (attempts < 2) {
                            setTimeout(() => { }, 5);
                            continue;
                        }
                        throw rollback;
                    }
                    if (attempts < 2) {
                        setTimeout(() => { }, 5);
                        continue;
                    }
                    throw action;
                }
            }
            throw new TransactionException(
                "Failed to execute transaction after multiple attempts",
            );
        } finally {
            if (this._type === 'pool') {
                try {
                    (client as PoolConnection).release();
                } catch {
                    console.warn('failed to realese pool client')
                }
            }
        }
    }
}

export class MariaDB extends BaseAdapter implements IAdapter {
    protected client: IClient;

    constructor(pool: PoolOptions | Connection) {
        super({
            type: 'mariadb'
        });
        this.client = new MariaClient(pool);
        this.setMeta({
            database: this.client.$database
        });
    }

    async create(name: string): Promise<void> {
        name = this.quote(name);
        if (await this.exists(name)) return;

        let sql = `CREATE DATABASE ${name} /*!40100 DEFAULT CHARACTER SET utf8mb4 */;`;
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
        const hash: Record<string, typeof attributes[number]> = {};

        attributes.forEach(attribute => {
            const id = this.sanitize(attribute.getId());
            hash[id] = attribute;

            const type = this.getSQLType(
                attribute.get('type'),
                attribute.get('size'),
                attribute.get('signed'),
                attribute.get('array')
            );

            if (attribute.get('type') === AttributeEnum.Virtual) {
                return;
            }

            if (attribute.get('type') === AttributeEnum.Relation) {
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

            let sql = `${this.quote(id)} ${type}`;
            attributeSql.push(sql);
        })

        indexes?.forEach((index, key) => {
            const indexId = this.sanitize(index.getId());
            const indexType = index.get('type');
            let indexAttributes = index.get('attributes') as string[];
            const lengths = index.get('lengths') || [];
            const orders = index.get('orders') || [];

            indexAttributes = indexAttributes.map((attribute, nested) => {
                let indexLength = lengths[nested] ?? '';
                indexLength = indexLength ? `(${Number(indexLength)})` : '';
                let indexOrder = orders[nested] ?? '';
                let indexAttribute = this.quote(
                    this.getInternalKeyForAttribute(attribute)
                );

                if (indexType === IndexEnum.FullText) {
                    indexOrder = '';
                }

                let attrSql = `${indexAttribute}${indexLength}${indexOrder ? ' ' + indexOrder : ''}`;

                if (
                    hash[indexAttribute]?.get('array', false) &&
                    this.$supportForCastIndexArray
                ) {
                    attrSql = `(CAST(\`${indexAttribute}\` AS char(${Database.ARRAY_INDEX_LENGTH}) ARRAY))`;
                }

                return attrSql;
            });

            let attributesSql = indexAttributes.join(', ');

            if (this.$sharedTables && indexType !== IndexEnum.FullText) {
                attributesSql = `_tenant, ${attributesSql}`;
            }

            indexSql.push(`${indexType} \`${indexId}\` (${attributesSql})`);
        });

        const columns = [
            '`_id` INT(11) UNSIGNED NOT NULL AUTO_INCREMENT',
            '`_uid` VARCHAR(255) NOT NULL',
            '`_createdAt` DATETIME(3) DEFAULT NULL',
            '`_updatedAt` DATETIME(3) DEFAULT NULL',
            '`_permissions` MEDIUMTEXT DEFAULT NULL',
            ...attributeSql
        ];

        let tableSql = `
            CREATE TABLE ${this.quote(name)} (
            ${columns.join(',\n')},
            PRIMARY KEY (_id)
            ${indexSql.length ? ',\n' + indexSql.join(',\n') : ''}
        `;

        if (this.$sharedTables) {
            tableSql += `,
            _tenant INT(11) UNSIGNED DEFAULT NULL,
            UNIQUE KEY _uid (_uid, _tenant),
            KEY _created_at (_tenant, _createdAt),
            KEY _updated_at (_tenant, _updatedAt),
            KEY _tenant_id (_tenant, _id)
            `;
        } else {
            tableSql += `,
            UNIQUE KEY _uid (_uid),
            KEY _created_at (_createdAt),
            KEY _updated_at (_updatedAt)
            `;
        }

        tableSql += `
            )
        `;
        tableSql = this.trigger(EventsEnum.CollectionCreate, tableSql);

        let permissionsTable = `
                CREATE TABLE ${this.quote(name + '_perms')} (
                    _id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT,
                    _type VARCHAR(12) NOT NULL,
                    _permission VARCHAR(255) NOT NULL,
                    _document VARCHAR(255) NOT NULL,
                    PRIMARY KEY (_id),
            `;

        if (this.$sharedTables) {
            permissionsTable += `
                    _tenant INT(11) UNSIGNED DEFAULT NULL,
                    UNIQUE INDEX _index1 (_document, _tenant, _type, _permission),
                    INDEX _permission (_tenant, _permission, _type)
                `;
        } else {
            permissionsTable += `
                    UNIQUE INDEX _index1 (_document, _type, _permission),
                    INDEX _permission (_permission, _type)
                `;
        }

        permissionsTable += `
                )
            `;
        permissionsTable = this.trigger(EventsEnum.CollectionCreate, permissionsTable);

        await this.client.query(tableSql);
        await this.client.query(permissionsTable);
    }

    public async getSizeOfCollectionOnDisk(collection: string): Promise<number> {
        collection = this.sanitize(collection);
        const database = this.client.$database;
        collection = `${this.$namespace}_${collection}`;
        const collectionName = `${database}/${collection}`;
        const permissionsName = `${database}/${collection}_perms`;

        const sql = `
            SELECT SUM(FS_BLOCK_SIZE + ALLOCATED_SIZE) as size
            FROM INFORMATION_SCHEMA.INNODB_SYS_TABLESPACES
            WHERE NAME = ?
        `;

        try {
            const [collectionRows]: any = await this.client.query(sql, [collectionName]);
            const [permissionsRows]: any = await this.client.query(sql, [permissionsName]);
            const collectionSize = collectionRows[0]?.size ?? 0;
            const permissionsSize = permissionsRows[0]?.size ?? 0;
            return Number(collectionSize) + Number(permissionsSize);
        } catch (e: any) {
            throw new DatabaseException(`Failed to get size of collection ${collection}: ${e.message}`);
        }
    }

    public async getSizeOfCollection(collection: string): Promise<number> {
        collection = this.sanitize(collection);
        const database = this.client.$database;
        const tableName = `${this.$namespace}_${collection}`;
        const permissionsName = `${tableName}_perms`;

        const sql = `
            SELECT SUM(data_length + index_length) as size
            FROM INFORMATION_SCHEMA.TABLES
            WHERE table_name = ? AND table_schema = ?
        `;

        try {
            const [collectionRows]: any = await this.client.query(sql, [tableName, database]);
            const [permissionsRows]: any = await this.client.query(sql, [permissionsName, database]);
            const collectionSize = collectionRows[0]?.size ?? 0;
            const permissionsSize = permissionsRows[0]?.size ?? 0;
            return Number(collectionSize) + Number(permissionsSize);
        } catch (e: any) {
            this.processException(e, `Failed to get size of collection ${collection}: ${e.message}`);
        }
    }

    public async deleteCollection(id: string): Promise<void> {
        id = this.sanitize(id);

        let sql = `DROP TABLE ${this.getSQLTable(id)}, ${this.getSQLTable(this.sanitize(id + '_perms'))};`;
        sql = this.trigger(EventsEnum.CollectionDelete, sql);

        await this.client.query(sql);
    }

    public async analyzeCollection(collection: string): Promise<boolean> {
        const name = this.sanitize(collection);
        const sql = `ANALYZE TABLE ${this.getSQLTable(name)}`;

        try {
            await this.client.query(sql);
            return true;
        } catch {
            return false;
        }
    }

    public async getSchemaAttributes(collection: string): Promise<Doc<ColumnInfo>[]> {
        const schema = this.client.$database;
        const table = `${this.$namespace}_${this.sanitize(collection)}`;

        const sql = `
            SELECT
                COLUMN_NAME as _id,
                COLUMN_DEFAULT as columnDefault,
                IS_NULLABLE as isNullable,
                DATA_TYPE as dataType,
                CHARACTER_MAXIMUM_LENGTH as characterMaximumLength,
                NUMERIC_PRECISION as numericPrecision,
                NUMERIC_SCALE as numericScale,
                DATETIME_PRECISION as datetimePrecision,
                COLUMN_TYPE as columnType,
                COLUMN_KEY as columnKey,
                EXTRA as extra
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        `;

        try {
            const [rows]: any = await this.client.query(sql, [schema, table]);
            return rows.map((row: any) => {
                row['$id'] = row['_id'];
                delete row['_id'];
                return Doc.from(row);
            });
        } catch (e: any) {
            this.processException(e, 'Failed to get schema attributes');
        }
    }

    public async updateAttribute(
        { collection, name, newName, array, size, signed, type }: UpdateAttribute
    ): Promise<void> {
        const tableName = this.getSQLTable(this.sanitize(collection));
        const columnName = this.sanitize(name);
        const newColumnName = newName ? this.sanitize(newName) : null;
        const sqlType = this.getSQLType(type, size, signed, array);

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

    protected getSQLType(type: AttributeEnum, size: number, signed?: boolean, array?: boolean): string {
        if (array) {
            return `JSON`;
        }
        switch (type) {
            case AttributeEnum.String:
                // size = size * 4; // Convert utf8mb4 size to bytes
                if (size > 16777215) {
                    return 'LONGTEXT';
                } else if (size > 65535) {
                    return `MEDIUMTEXT`;
                } else if (size > this.$maxVarcharLength) {
                    return `TEXT`;
                } else return `VARCHAR(${size})`;
            case AttributeEnum.Integer:
                const _signed = signed ? '' : ' UNSIGNED';
                if (size >= 8) {
                    return `BIGINT${_signed}`;
                }
                return `INT${_signed}`;
            case AttributeEnum.Float:
                return signed ? 'DOUBLE' : 'UNSIGNED DOUBLE';
            case AttributeEnum.Boolean:
                return 'TINYINT(1)';
            case AttributeEnum.Date:
                return 'DATETIME(3)';
            case AttributeEnum.Relation:
                return 'VARCHAR(255)';
            case AttributeEnum.Object:
                return 'JSON';
            case AttributeEnum.Virtual:
                return '';
            default:
                throw new DatabaseException(`Unsupported attribute type: ${type}`);
        }
    }

    public quote(name: string): string {
        if (!name) {
            throw new DatabaseException("Failed to quote name: name is empty");
        }
        return `\`${this.sanitize(name)}\``;
    }

    protected processException(error: any, message?: string): never {
        throw new DatabaseException('Not implemented')
    }
}
