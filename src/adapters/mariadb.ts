import { DatabaseException, TransactionException } from "@errors/index.js";
import { BaseAdapter } from "./base.js";
import { IAdapter, IClient } from "./interface.js";
import { createPool, Pool, Connection, PoolOptions, PoolConnection } from 'mysql2/promise';

class MariaClient implements IClient {
    private connection: Connection | Pool;
    private _type: 'connection' | 'pool' = 'connection';

    get $database(): string {
        return this.connection.config.database || '';
    }

    get $client() {
        return this.connection;
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
    protected client: MariaClient;

    constructor(pool: PoolOptions | Connection) {
        super();
        this.client = new MariaClient(pool);
        this.setMeta({
            database: this.client.$database
        });
    }

    async ping(): Promise<void> {
        try {
            await this.client.$client.ping()
        } catch (e) {
            throw new DatabaseException(`Ping failed.`)
        }
    }

    async create(name: string): Promise<void> {
        name = this.sanitize(name);
        if (await this.exists(name)) return;

        const sql = `CREATE DATABASE \`{ name }\` /*!40100 DEFAULT CHARACTER SET utf8mb4 */;`;
        await this.client.query(sql);
    }

    async exists(name: string, collection?: string): Promise<boolean> {
        const values: string[] = [this.sanitize(name)];
        let sql: string;

        if (collection) {
            // Check if collection exists
            sql = `
              SELECT COUNT(*) as count 
              FROM information_schema.tables 
              WHERE table_schema = ? 
              AND table_name = ?
            `;
            values.push(this.sanitize(collection));
        } else {
            // Check if schema exists
            sql = `
              SELECT COUNT(*) as count 
              FROM information_schema.schemata 
              WHERE schema_name = ?
            `;
        }

        const [result] = await this.client.query<any>(sql, values);
        return result[0].count > 0;
    }

    async delete(name: string): Promise<void> {
        name = this.sanitize(name);
        await this.client.query(`DROP SCHEMA IF EXISTS \`${name}\`;`);
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
