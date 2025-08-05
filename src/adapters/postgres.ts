import {
    Client, escapeLiteral, Pool, PoolClient,
    QueryArrayConfig, QueryArrayResult, QueryConfig,
    QueryConfigValues, QueryResult, QueryResultRow, Submittable,
    DatabaseError,
    PoolConfig
} from "pg";
import { IClient } from "./interface.js";
import { DatabaseException } from "@errors/base.js";
import { TransactionException } from "@errors/index.js";

export class PostgresClient implements IClient {
    private connection: Client | Pool | PoolClient;
    private _type: 'connection' | 'pool' | 'transaction' = 'connection';

    private readonly isTransactional: boolean = false;

    get $client(): Client | Pool | PoolClient {
        return this.connection;
    }

    get $type(): 'connection' | 'pool' | 'transaction' {
        return this._type;
    }

    constructor(options: PoolConfig | Client | { _internal_conn_: PoolClient | Client, _internal_type_: 'connection' }) {
        if ('_internal_conn_' in options) {
            this.connection = options._internal_conn_;
            this._type = options._internal_type_;
            this.isTransactional = true;
            return;
        }

        if ('connect' in options) {
            this.connection = options;
            this._type = 'connection';
        } else {
            this.connection = new Pool(options);
            this._type = 'pool';
        }
    }

    async connect(): Promise<void> { }

    async disconnect(): Promise<void> {
        if (this.isTransactional) {
            throw new DatabaseException("Cannot disconnect a client within a transaction.");
        }
        if (this._type === 'pool') {
            await (this.connection as Pool).end();
        } else if (this._type === 'connection') {
            await (this.connection as Client).end();
        } else if (this._type === 'transaction') {
            // For transactions, we do not end the connection here.
            // It will be released back to the pool after the transaction is completed.
            return;
        } else {
            throw new DatabaseException("Unknown connection type.");
        }
    }

    query<T extends Submittable>(queryStream: T): T;
    // tslint:disable:no-unnecessary-generics
    query<R extends any[] = any[], I = any[]>(
        queryConfig: QueryArrayConfig<I>,
        values?: QueryConfigValues<I>,
    ): Promise<QueryArrayResult<R>>;
    query<R extends QueryResultRow = any, I = any>(
        queryConfig: QueryConfig<I>,
    ): Promise<QueryResult<R>>;
    query<R extends QueryResultRow = any, I = any[]>(
        queryTextOrConfig: string | QueryConfig<I>,
        values?: QueryConfigValues<I>,
    ): Promise<QueryResult<R>>;
    public query(sql: any, values?: any): Promise<any> {
        // Replace ? placeholders with $1, $2, $3, etc. for PostgreSQL
        if (typeof sql === 'string' && values && Array.isArray(values)) {
            let paramIndex = 1;
            const convertedSql = sql.replace(/\?/g, () => `$${paramIndex++}`);
            return this.connection.query(convertedSql, values);
        }
        return this.connection.query(sql, values);
    }

    public quote(value: any): string {
        return escapeLiteral(value);
    }

    async ping(): Promise<void> {
        try {
            await this.query('SELECT 1');
        } catch (e) {
            throw new DatabaseException(`Ping failed.`);
        }
    }

    async transaction<T>(
        callback: (client: Omit<IClient, 'transaction' | 'disconnect'>) => Promise<T>,
        maxRetries = 3
    ): Promise<T> {
        if (this.isTransactional) {
            throw new TransactionException('Cannot start a nested transaction.');
        }

        const conn = this._type === 'pool'
            ? await (this.connection as Pool).connect()
            : this.connection as Client;

        try {
            for (let attempt = 1; attempt <= maxRetries; attempt++) {
                try {
                    await this.query('BEGIN');
                    const transactionalClient = new PostgresClient(
                        { _internal_conn_: conn, _internal_type_: 'connection' }
                    );

                    const result = await callback(transactionalClient);
                    await this.query('COMMIT');

                    return result;
                } catch (err: unknown) {
                    console.warn(`Transaction attempt ${attempt} failed:`, err);
                    await this.query('ROLLBACK');
                    const isDeadlock = err instanceof DatabaseError && 'code' in err && err.code === '40P01';
                    if (isDeadlock && attempt < maxRetries) {
                        await new Promise(resolve => setTimeout(resolve, 50 * attempt));
                        continue;
                    }
                    throw err;
                }
            }
            throw new TransactionException(`Transaction failed after ${maxRetries} attempts.`);
        } finally {
            if ('release' in conn) {
                conn.release();
            }
        }
    }
}
