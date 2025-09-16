import PG, {
  Client,
  escapeLiteral,
  Pool,
  PoolClient,
  QueryArrayConfig,
  QueryArrayResult,
  QueryConfig,
  QueryConfigValues,
  QueryResult,
  QueryResultRow,
  Submittable,
  DatabaseError,
  PoolConfig,
} from "pg";
import { IClient } from "./interface.js";
import { DatabaseException } from "@errors/base.js";
import { TransactionException } from "@errors/index.js";
import { Logger } from "@utils/logger.js";

const types = PG.types;

types.setTypeParser(types.builtins.INT8, (x) => {
  const asNumber = Number(x);
  return Number.isSafeInteger(asNumber) ? asNumber : x;
});

types.setTypeParser(types.builtins.NUMERIC, parseFloat);
types.setTypeParser(types.builtins.FLOAT4, parseFloat);
types.setTypeParser(types.builtins.FLOAT8, parseFloat);
types.setTypeParser(types.builtins.BOOL, (val) => val === "t");

types.setTypeParser(types.builtins.DATE, (x) => x);
types.setTypeParser(types.builtins.TIMESTAMP, (x) => x);
types.setTypeParser(types.builtins.TIMESTAMPTZ, (x) => x);
types.setTypeParser(types.builtins.INTERVAL, (x) => x);

types.setTypeParser(600 as any, (x) => x); // point
types.setTypeParser(1017 as any, (x) => x); // _point

const timestampzParser = (x: string | null): Date | null => {
  if (x === null) return null;
  const date = new Date(x);
  return isNaN(date.getTime()) ? null : date;
};

const getTypeParser = (id: any, format: any) => {
  if (id === types.builtins.TIMESTAMPTZ && format === "text") {
    return timestampzParser;
  }
  return types.getTypeParser(id, format);
};

export class PostgresClient implements IClient {
  private connection: Client | Pool | PoolClient;
  private pool: Pool | null = null;
  private _type: "connection" | "pool" | "transaction" = "connection";
  private isTransactional: boolean = false;
  private transactionCount: number = 0;
  private _database: string;

  get $client(): Client | Pool | PoolClient {
    return this.connection;
  }

  get $type(): "connection" | "pool" | "transaction" {
    return this._type;
  }

  get $database(): string {
    return this._database;
  }

  constructor(options: PoolConfig | Client) {
    if ("connect" in options) {
      this.connection = options;
      this._type = "connection";
    } else {
      const pool = new Pool({
        ...options,
      });
      this.connection = pool;
      this.pool = pool;
      this._type = "pool";
    }
    this._database = options.database || "";
  }

  async connect(): Promise<void> {}

  async disconnect(): Promise<void> {
    if (this.isTransactional) {
      throw new DatabaseException(
        "Cannot disconnect a client within a transaction.",
      );
    }
    if (this._type === "pool" && this.pool) {
      await this.pool.end();
    } else if (this._type === "connection") {
      await (this.connection as Client).end();
    } else if (this._type === "transaction") {
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
    if (typeof sql === "string" && values && Array.isArray(values)) {
      let paramIndex = 1;
      const convertedSql = sql.replace(/\?/g, () => `$${paramIndex++}`);
      return this.connection.query({
        text: convertedSql,
        values,
        types: { getTypeParser },
      });
    }
    return this.connection.query({
      text: typeof sql === "string" ? sql : sql.text,
      values: typeof sql === "string" ? values : sql.values,
      types: { getTypeParser },
    });
  }

  public quote(value: any): string {
    return escapeLiteral(value);
  }

  async ping(): Promise<void> {
    try {
      await this.query("SELECT 1");
    } catch (e) {
      throw new DatabaseException(`Ping failed.`);
    }
  }

  async beginTransaction(): Promise<void> {
    if (this.transactionCount === 0) {
      if (this._type === "pool" && this.pool) {
        this.connection = await this.pool.connect();
        this._type = "transaction";
        this.isTransactional = true;
      }
      await this.query("BEGIN");
    } else {
      await this.query(`SAVEPOINT sp_${this.transactionCount}`);
    }
    this.transactionCount++;
  }

  async commit(): Promise<void> {
    if (this.transactionCount === 0) {
      throw new TransactionException("No active transaction to commit.");
    }

    this.transactionCount--;
    if (this.transactionCount === 0) {
      await this.query("COMMIT");
      await this._cleanupTransaction();
    } else {
      await this.query(`RELEASE SAVEPOINT sp_${this.transactionCount}`);
    }
  }

  async rollback(): Promise<void> {
    if (this.transactionCount === 0) {
      throw new TransactionException("No active transaction to rollback.");
    }

    this.transactionCount--;
    if (this.transactionCount === 0) {
      await this.query("ROLLBACK");
      await this._cleanupTransaction();
    } else {
      await this.query(`ROLLBACK TO SAVEPOINT sp_${this.transactionCount}`);
    }
  }

  private async _cleanupTransaction(): Promise<void> {
    if (
      this._type === "transaction" &&
      "release" in this.connection &&
      this.pool
    ) {
      const poolConnection = this.connection;
      poolConnection.release();
      this.connection = this.pool;
      this._type = "pool";
    }
    this.isTransactional = false;
  }

  async transaction<T>(callback: () => Promise<T>, maxRetries = 3): Promise<T> {
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        await this.beginTransaction();
        const result = await callback();
        await this.commit();
        return result;
      } catch (err: unknown) {
        Logger.warn(`Transaction attempt ${attempt} failed:`, err);
        await this.rollback();
        const isDeadlock =
          err instanceof DatabaseError && "code" in err && err.code === "40P01";
        if (isDeadlock && attempt < maxRetries) {
          await new Promise((resolve) => setTimeout(resolve, 50 * attempt));
          continue;
        }
        throw err;
      }
    }
    throw new TransactionException(
      `Transaction failed after ${maxRetries} attempts.`,
    );
  }
}
