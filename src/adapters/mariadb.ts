import { DatabaseException, TransactionException } from "@errors/index.js";
import { BaseAdapter } from "./base.js";
import { CreateCollectionOptions, IAdapter, IClient } from "./interface.js";
import { createPool, Pool, Connection, PoolOptions, PoolConnection } from 'mysql2/promise';
import { AttributeEnum } from "@core/enums.js";

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
        name = this.sanitize(name);
        if (await this.exists(name)) return;

        const sql = `CREATE DATABASE \`{ name }\` /*!40100 DEFAULT CHARACTER SET utf8mb4 */;`;
        await this.client.query(sql);
    }

    async delete(name: string): Promise<void> {
        name = this.sanitize(name);
        await this.client.query(`DROP SCHEMA IF EXISTS \`${name}\`;`);
    }

    async createCollection({ name, attributes, indexes, documentSecurity }: CreateCollectionOptions): Promise<void> {
        name = this.sanitize(name);
        const attributeSql = [];
        const indexSql = [];
        const hash: Record<string, typeof attributes[number]> = {};

        attributes.forEach(attribute => {
            const id = this.sanitize(attribute.getId());
            hash[id] = attribute;

        })

        indexes?.forEach(index => {

        })
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

    // Capabilities
    get $supportsCastIndexArray(): boolean {
        return false;
    }

    get $supportsIndex(): boolean {
        return true;
    }

    get $supportsUniqueIndex(): boolean {
        return true;
    }

    get $supportsFulltextIndex(): boolean {
        return true;
    }

    get $supportsFulltextWildcardIndex(): boolean {
        return true;
    }

    get $supportsTimeouts(): boolean {
        return true;
    }

    get $supportsCasting(): boolean {
        return false;
    }

    get $supportsJSONOverlaps(): boolean {
        return false;
    }

    // Limits
    get $limitString(): number {
        return 4294967295; // TODO: ------
    }

    get $limitInt(): number {
        return 4294967295;
    }

    get $limitAttributes(): number {
        return 1017;
    }

    get $limitIndexes(): number {
        return 64;
    }

    // Max Sizes
    get $maxVarcharLength(): number {
        return 16381;
    }

    get $maxIndexLength(): number {
        return 768;
    }
}
