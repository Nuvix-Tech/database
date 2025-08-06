import PG, {
    Client, escapeLiteral, Pool, PoolClient,
    QueryArrayConfig, QueryArrayResult, QueryConfig,
    QueryConfigValues, QueryResult, QueryResultRow, Submittable,
    DatabaseError,
    PoolConfig
} from "pg";
import { IClient } from "./interface.js";
import { DatabaseException } from "@errors/base.js";
import { TransactionException } from "@errors/index.js";

const types = PG.types;

types.setTypeParser(types.builtins.INT8, x => {
    const asNumber = Number(x);
    return Number.isSafeInteger(asNumber) ? asNumber : x;
});

types.setTypeParser(types.builtins.NUMERIC, parseFloat);
types.setTypeParser(types.builtins.FLOAT4, parseFloat);
types.setTypeParser(types.builtins.FLOAT8, parseFloat);
types.setTypeParser(types.builtins.BOOL, val => val === 't');

types.setTypeParser(types.builtins.DATE, x => x);
types.setTypeParser(types.builtins.TIMESTAMP, x => x);
types.setTypeParser(types.builtins.TIMESTAMPTZ, x => x);
types.setTypeParser(types.builtins.INTERVAL, x => x);

// types.setTypeParser(1115 as any, parseArray); // _timestamp[]
// types.setTypeParser(1182 as any, parseArray); // _date[]
// types.setTypeParser(1185 as any, parseArray); // _timestamptz[]
types.setTypeParser(600 as any, x => x); // point
types.setTypeParser(1017 as any, x => x); // _point

export class PostgresClient implements IClient {
    private connection: Client | Pool | PoolClient;
    private _type: 'connection' | 'pool' | 'transaction' = 'connection';
    private isTransactional: boolean = false;

    get $client(): Client | Pool | PoolClient {
        return this.connection;
    }

    get $type(): 'connection' | 'pool' | 'transaction' {
        return this._type;
    }

    constructor(options: PoolConfig | Client) {
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
            return;
        } else {
            throw new DatabaseException("Unknown connection type.");
        }
    }

    query<T extends Submittable>(queryStream: T): T;
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
        callback: () => Promise<T>,
        maxRetries = 3
    ): Promise<T> {
        if (this.isTransactional) {
            throw new TransactionException('Cannot start a nested transaction.');
        }

        const originalConnection = this.connection;
        const originalType = this._type;

        if (this._type === 'pool') {
            this.connection = await (this.connection as Pool).connect();
            this._type = 'transaction';
            this.isTransactional = true;
        }

        try {
            for (let attempt = 1; attempt <= maxRetries; attempt++) {
                try {
                    await this.query('BEGIN');
                    const result = await callback();
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
            if (originalType === 'pool' && 'release' in this.connection) {
                (this.connection as PoolClient).release();
            }
            this.connection = originalConnection;
            this._type = originalType;
            this.isTransactional = false;
        }
    }
}
