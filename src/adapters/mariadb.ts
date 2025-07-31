import { TransactionException } from "@errors/index.js";
import { BaseAdapter } from "./base.js";
import { IAdapter, IClient } from "./interface.js";
import { createPool, Pool, Connection, PoolOptions, PoolConnection } from 'mysql2/promise';

class MariaClient implements IClient {
    private connection: Connection | Pool;
    private _type: 'connection' | 'pool' = 'connection';

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
    }
}
