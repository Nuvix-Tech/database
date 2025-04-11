import { Document } from "../core/Document";
import { Query } from "../core/query";
import { Adapter } from "./base";
import { Sql } from "./sql";
import { Pool, PoolConfig, PoolClient } from "pg";
import Transaction from "../errors/Transaction";
import { Database } from "../core/database";
import {
    DatabaseError,
    DuplicateException,
    InitializeError,
    TimeoutException,
    TruncateException,
} from "../errors";
import { Authorization } from "../security/authorization";

interface PostgreDBOptions {
    connection: PoolConfig | Pool;
    schema?: string;
}

// Extend Sql which should extend DatabaseAdapter
export class PostgreDB extends Sql implements Adapter {
    /**
     * @description PostgreSQL connection pool
     */
    private pool: Pool;

    /**
     * instance of PostgreDB
     */
    private instance: this | null = null;

    private timeout: number;

    /**
     * Pool statistics for monitoring
     */
    private poolStats = {
        totalConnections: 0,
        idleConnections: 0,
        waitingClients: 0,
        lastChecked: 0,
    };

    /**
     * Default pool configuration
     */
    private defaultPoolConfig: Partial<PoolConfig> = {
        max: 20, // Maximum number of clients
        idleTimeoutMillis: 30000, // How long a client can stay idle (30 sec)
        connectionTimeoutMillis: 10000, // Connection timeout (10 sec)
        allowExitOnIdle: true, // Allow the pool to exit when idle
    };

    /**
     * @description PostgreSQL connection options
     */
    declare protected options: PostgreDBOptions;

    /**
     * Query statistics tracking
     */
    private queryStats = {
        totalQueries: 0,
        successfulQueries: 0,
        failedQueries: 0,
        totalTimeMs: 0,
        slowestQueryMs: 0,
        slowestQuery: "",
        queriesPerSecond: 0,
        lastCalculated: 0,
        queryLog: [] as Array<{
            sql: string;
            timeMs: number;
            timestamp: number;
            success: boolean;
        }>,
    };

    /**
     * Maximum size of query log (keep memory usage reasonable)
     */
    private maxQueryLogSize = 100;

    /**
     * Stats calculation interval
     */
    private statsInterval = 5000; // 5 seconds

    constructor(options: PostgreDBOptions) {
        super();
        this.options = options;
        this.type = "postgresql";
        this.database = options.schema ?? "public";
    }

    isInitialized(): boolean {
        return this.instance !== null;
    }

    public init() {
        if (this.instance)
            throw new InitializeError("PostgreSql adapter already initialized");
        try {
            // Prepare connection config with defaults
            let poolConfig: PoolConfig;

            if (this.options.connection instanceof Pool) {
                this.pool = this.options.connection;
                poolConfig = this.pool.options;
            } else {
                // Apply default configuration while preserving user settings
                poolConfig = {
                    ...this.defaultPoolConfig,
                    ...this.options.connection,
                };

                this.pool = new Pool(poolConfig);
            }

            this.instance = this;

            // Setup pool event handlers
            this.setupPoolListeners();

            // Start periodic pool health check
            this.startPoolHealthCheck();

            this.logger.info(
                `PostgreSQL pool initialized with max ${poolConfig.max} connections`,
            );
        } catch (e) {
            this.logger.error(e);
            throw new InitializeError(
                "PostgreSQL adapter initialization failed",
            );
        }
    }

    /**
     * Setup event listeners for the connection pool
     */
    private setupPoolListeners() {
        // Handle pool errors
        this.pool.on("error", (err, client) => {
            this.logger.error("Unexpected error on idle client", err);
            // Remove the problematic client from the pool
            if (client) {
                try {
                    client.release(true); // Force release with error
                } catch (e) {
                    this.logger.error(
                        "Error while releasing problematic client",
                        e,
                    );
                }
            }
        });

        // Track connection acquisitions
        this.pool.on("connect", (client) => {
            this.poolStats.totalConnections++;
        });

        // Track when clients are removed from the pool
        this.pool.on("remove", (client) => {
            if (this.poolStats.totalConnections > 0) {
                this.poolStats.totalConnections--;
            }
        });
    }

    /**
     * Start periodic health check of the connection pool
     */
    private startPoolHealthCheck() {
        const checkInterval = 60000 * 10; // Check every minute

        // Don't create intervals in test environments
        if (process.env.NODE_ENV === "test") {
            return;
        }

        const intervalId = setInterval(async () => {
            try {
                // Only check if initialized
                if (!this.instance) {
                    return;
                }

                const idleCount = this.pool.idleCount;
                const totalCount = this.pool.totalCount;
                const waitingCount = this.pool.waitingCount;

                this.poolStats = {
                    totalConnections: totalCount,
                    idleConnections: idleCount,
                    waitingClients: waitingCount,
                    lastChecked: Date.now(),
                };

                // Log if pool is under pressure (many waiting clients)
                if (waitingCount > 5) {
                    this.logger.warn(
                        `PostgreSQL pool pressure: ${waitingCount} clients waiting, ${totalCount} total connections, ${idleCount} idle`,
                    );
                }

                // Ping database to ensure connection is still valid
                // await this.ping();
            } catch (err) {
                this.logger.error("Error in pool health check:", err);
            }
        }, checkInterval);

        // Store for cleanup
        this.cleanupCallbacks.push(() => {
            clearInterval(intervalId);
        });
    }

    // Store cleanup functions
    private cleanupCallbacks: Array<() => void> = [];

    /**
     * Get current pool statistics
     */
    public getPoolStats() {
        return {
            ...this.poolStats,
            totalCount: this.pool?.totalCount,
            idleCount: this.pool?.idleCount,
            waitingCount: this.pool?.waitingCount,
        };
    }

    /**
     * Acquire a client with timeout
     * @returns A PostgreSQL client from the pool
     */
    public async getClient(): Promise<any> {
        if (!this.pool) {
            throw new DatabaseError("No PostgreSQL connection pool available");
        }

        try {
            const client = await this.pool.connect();
            return client;
        } catch (err: any) {
            this.logger.error(
                "Failed to acquire database client from pool:",
                err,
            );
            throw new DatabaseError(
                `Failed to acquire database client: ${err.message}`,
            );
        }
    }

    /**
     * Improved version of withTransaction that properly manages client resources
     */
    public async withTransaction<T>(
        callback: (client?: any) => Promise<T>,
    ): Promise<T> {
        let client;

        try {
            client = await this.getClient();
            await this.startTransaction(client);

            const result = await callback(client);

            await this.commitTransaction(client);
            return result;
        } catch (err) {
            if (client && this.inTransaction > 0) {
                try {
                    await this.rollbackTransaction(client);
                } catch (rollbackErr) {
                    this.logger.error(
                        "Error rolling back transaction:",
                        rollbackErr,
                    );
                }
            }
            throw err;
        } finally {
            if (client) {
                client.release();
            }
        }
    }

    /**
     * Ping database to check connection
     * @returns Promise that resolves when connection is confirmed
     */
    ping(): Promise<void> {
        return new Promise(async (resolve, reject) => {
            try {
                await this.executeQuery(
                    "SELECT 1 AS ping",
                    [],
                    undefined,
                    "ping",
                );
                resolve();
            } catch (err) {
                reject(err);
            }
        });
    }

    /**
     * Clean up resources when closing the adapter
     */
    async close(): Promise<void> {
        // Run all cleanup callbacks
        for (const cleanup of this.cleanupCallbacks) {
            try {
                cleanup();
            } catch (err) {
                this.logger.error("Error during cleanup:", err);
            }
        }

        if (this.pool) {
            // Wait for all clients to be released
            await this.pool.end();
            this.instance = null;
            this.logger.info("PostgreSQL connection pool has been closed");
        }
    }

    /**
     * Starts a database transaction
     * @returns Promise resolving to true if transaction started successfully
     * @throws Error if transaction could not be started
     */
    async startTransaction(client: any): Promise<boolean> {
        try {
            if (this.inTransaction === 0) {
                // If there's a lingering transaction, roll it back
                try {
                    await client.query("ROLLBACK");
                } catch (e) {
                    // Ignore errors from rollback of non-existent transaction
                }

                // Start a new transaction
                await client.query("BEGIN");
            }

            this.inTransaction++;
            return true;
        } catch (e: any) {
            throw new Transaction(`Failed to start transaction: ${e.message}`);
        }
    }

    /**
     * Commits the current transaction
     * @returns Promise resolving to true if commit successful
     * @throws Error if commit fails
     */
    async commitTransaction(client: any): Promise<boolean> {
        if (this.inTransaction === 0) {
            return false;
        }

        try {
            this.inTransaction--;

            if (this.inTransaction === 0) {
                await client.query("COMMIT");
            }

            return true;
        } catch (e: any) {
            throw new Transaction(`Failed to commit transaction: ${e.message}`);
        }
    }

    /**
     * Rolls back the current transaction
     * @returns Promise resolving to true if rollback successful
     * @throws Error if rollback fails
     */
    async rollbackTransaction(client: any): Promise<boolean> {
        if (this.inTransaction === 0) {
            return false;
        }

        try {
            await client.query("ROLLBACK");
            this.inTransaction = 0;
            return true;
        } catch (e: any) {
            throw new Transaction(
                `Failed to rollback transaction: ${e.message}`,
            );
        }
    }

    /**
     * Creates a new database schema
     * @param name - The name of the schema to create
     * @returns Promise resolving to true if schema created successfully or already exists
     * @throws Error if schema creation fails
     */
    async create(name: string): Promise<boolean> {
        const filteredName = this.filter(name);

        // Check if schema already exists
        if (await this.exists(filteredName)) {
            return true;
        }

        const sql = `CREATE SCHEMA "${filteredName}"`;
        const triggeredSql = await this.trigger(
            Database.EVENT_DATABASE_CREATE,
            sql,
        );

        try {
            await this.executeQuery(
                triggeredSql,
                [],
                undefined,
                "create_schema",
            );
            return true;
        } catch (e: any) {
            throw new Error(`Failed to create schema: ${e.message}`);
        }
    }

    setDatabase(database: string): void {
        this.database = database;
    }

    use(name: string): Promise<boolean> {
        // noop
        return Promise.resolve(true);
    }

    /**
     *  Get Tenant SQL with proper parameter binding for PostgreSQL
     */
    private getTenantSql(paramIndex: number = 1) {
        return `AND (_tenant = $${paramIndex} OR _tenant IS NULL)`;
    }

    /**
     * Drops a database schema
     * @param name - The name of the schema to drop
     * @returns Promise resolving to true if schema dropped successfully
     * @throws Error if schema drop fails
     */
    async drop(name: string): Promise<boolean> {
        const filteredName = this.filter(name);

        const sql = `DROP SCHEMA IF EXISTS "${filteredName}" CASCADE`;
        const triggeredSql = await this.trigger(
            Database.EVENT_DATABASE_DELETE,
            sql,
        );

        try {
            await this.executeQuery(triggeredSql, [], undefined, "drop_schema");
            return true;
        } catch (e: any) {
            throw new DatabaseError(`Failed to drop schema: ${e.message}`);
        }
    }

    /**
     * Checks if a database schema or table exists
     * @param name - The name of the schema or table to check
     * @param collection - If provided, checks if this table exists within the schema
     * @returns Promise resolving to true if schema/table exists, false otherwise
     */
    async exists(name: string, collection?: string): Promise<boolean> {
        // Throw if name is not provided
        if (!name) {
            throw new DatabaseError(
                "Name parameter is required for exists check",
            );
        }

        try {
            if (collection) {
                // Check if table exists
                const query = `
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = $1 AND table_name = $2
                    );
                `;
                const result = await this.executeQuery(
                    query,
                    [name, collection],
                    undefined,
                    "exists_table",
                );
                return result.rows[0].exists;
            } else {
                // Check if schema exists
                const query = `
                    SELECT EXISTS (
                        SELECT FROM information_schema.schemata 
                        WHERE schema_name = $1
                    );
                `;
                const result = await this.executeQuery(
                    query,
                    [name],
                    undefined,
                    "exists_schema",
                );
                return result.rows[0].exists;
            }
        } catch (err) {
            this.logger.error(
                `Error checking if ${collection ? "table" : "schema"} exists:`,
                err,
            );
            throw err;
        }
    }

    /**
     * Creates a new collection (table) in the database
     * @param name - The name of the collection to create
     * @param attributes - Array of attribute documents defining the collection schema
     * @param indexes - Array of index documents defining indexes
     * @param ifExists - Whether to ignore if the collection already exists
     * @returns Promise resolving to true if collection created successfully
     */
    async createCollection(
        name: string,
        attributes: Document[] = [],
        indexes: Document[] = [],
        ifExists = false,
    ): Promise<boolean> {
        if (!name) {
            throw new DatabaseError("Collection name is required");
        }

        const prefix = this.getPrefix();
        const id = this.filter(name);
        const attributeStrings: string[] = [];
        let sqlStatements: string[] = [];

        for (const attribute of attributes) {
            // Get attribute ID from either $id or key or both
            const attrId = this.filter(
                attribute.getAttribute("key") || attribute.getAttribute("$id"),
            );

            if (!attrId) {
                this.logger.warn(
                    `Skipping attribute without ID in collection ${name}`,
                );
                continue;
            }

            const attrType = this.getSQLType(
                attribute.getAttribute("type"),
                attribute.getAttribute("size", 0),
                attribute.getAttribute("signed", true),
                attribute.getAttribute("array", false),
            );

            // Ignore relationships with virtual attributes
            if (attribute.getAttribute("type") === Database.VAR_RELATIONSHIP) {
                const options = attribute.getAttribute("options", {});
                const relationType = options["relationType"] || null;
                const twoWay = options["twoWay"] || false;
                const side = options["side"] || null;

                if (
                    relationType === Database.RELATION_MANY_TO_MANY ||
                    (relationType === Database.RELATION_ONE_TO_ONE &&
                        !twoWay &&
                        side === Database.RELATION_SIDE_CHILD) ||
                    (relationType === Database.RELATION_ONE_TO_MANY &&
                        side === Database.RELATION_SIDE_PARENT) ||
                    (relationType === Database.RELATION_MANY_TO_ONE &&
                        side === Database.RELATION_SIDE_CHILD)
                ) {
                    continue;
                }
            }

            attributeStrings.push(`"${attrId}" ${attrType}`);
        }

        const sqlTenant = this.getSharedTables()
            ? "_tenant INTEGER DEFAULT NULL,"
            : "";

        // Create table statement
        sqlStatements.push(`
      CREATE TABLE ${this.getSQLTable(id)} (
        _id SERIAL NOT NULL,
        _uid VARCHAR(255) NOT NULL,
        ${sqlTenant}
        "_createdAt" TIMESTAMP(3) DEFAULT NULL,
        "_updatedAt" TIMESTAMP(3) DEFAULT NULL,
        _permissions TEXT DEFAULT NULL,
        ${attributeStrings.join(", ")}${attributeStrings.length ? "," : ""}
        PRIMARY KEY (_id)
      )
    `);

        // Index statements
        if (this.sharedTables) {
            sqlStatements.push(
                `CREATE UNIQUE INDEX "${prefix}_${this.getTenantId()}_${id}_uid" ON ${this.getSQLTable(id)} (LOWER(_uid), _tenant)`,
            );
            sqlStatements.push(
                `CREATE INDEX "${prefix}_${this.getTenantId()}_${id}_created" ON ${this.getSQLTable(id)} (_tenant, "_createdAt")`,
            );
            sqlStatements.push(
                `CREATE INDEX "${prefix}_${this.getTenantId()}_${id}_updated" ON ${this.getSQLTable(id)} (_tenant, "_updatedAt")`,
            );
            sqlStatements.push(
                `CREATE INDEX "${prefix}_${this.getTenantId()}_${id}_tenant_id" ON ${this.getSQLTable(id)} (_tenant, _id)`,
            );
        } else {
            sqlStatements.push(
                `CREATE UNIQUE INDEX "${prefix}_${id}_uid" ON ${this.getSQLTable(id)} (LOWER(_uid))`,
            );
            sqlStatements.push(
                `CREATE INDEX "${prefix}_${id}_created" ON ${this.getSQLTable(id)} ("_createdAt")`,
            );
            sqlStatements.push(
                `CREATE INDEX "${prefix}_${id}_updated" ON ${this.getSQLTable(id)} ("_updatedAt")`,
            );
        }

        // Create permissions table
        sqlStatements.push(`
      CREATE TABLE ${this.getSQLTable(id + "_perms")} (
        _id SERIAL NOT NULL,
        _tenant INTEGER DEFAULT NULL,
        _type VARCHAR(12) NOT NULL,
        _permission VARCHAR(255) NOT NULL,
        _document VARCHAR(255) NOT NULL,
        PRIMARY KEY (_id)
      )
    `);

        // Permissions table indexes
        if (this.sharedTables) {
            sqlStatements.push(`CREATE UNIQUE INDEX "${prefix}_${this.getTenantId()}_${id}_ukey" 
        ON ${this.getSQLTable(id + "_perms")} USING btree (_tenant,_document,_type,_permission)`);
            sqlStatements.push(`CREATE INDEX "${prefix}_${this.getTenantId()}_${id}_permission" 
        ON ${this.getSQLTable(id + "_perms")} USING btree (_tenant,_permission,_type)`);
        } else {
            sqlStatements.push(`CREATE UNIQUE INDEX "${prefix}_${id}_ukey" 
        ON ${this.getSQLTable(id + "_perms")} USING btree (_document,_type,_permission)`);
            sqlStatements.push(`CREATE INDEX "${prefix}_${id}_permission" 
        ON ${this.getSQLTable(id + "_perms")} USING btree (_permission,_type)`);
        }

        sqlStatements = await this.trigger(
            Database.EVENT_COLLECTION_CREATE,
            sqlStatements,
        );

        try {
            // Execute each SQL statement individually
            for (const sql of sqlStatements) {
                await this.executeQuery(sql);
            }

            // Create indexes
            for (const index of indexes) {
                const indexId = this.filter(index.getAttribute("$id") || "");
                if (!indexId) {
                    this.logger.warn(
                        `Skipping index without ID in collection ${name}`,
                    );
                    continue;
                }

                const indexType = index.getAttribute("type");
                const indexAttributes = index.getAttribute("attributes", []);
                const indexOrders = index.getAttribute("orders", []);

                await this.createIndex(
                    id,
                    indexId,
                    indexType,
                    indexAttributes,
                    [],
                    indexOrders,
                );
            }

            return true;
        } catch (e: any) {
            const error = this.processException(e);

            if (!(error instanceof DuplicateException)) {
                try {
                    await this.executeQuery(
                        `DROP TABLE IF EXISTS ${this.getSQLTable(id)}`,
                    );
                    await this.executeQuery(
                        `DROP TABLE IF EXISTS ${this.getSQLTable(id + "_perms")}`,
                    );
                } catch {
                    // Ignore drop errors
                }
            }

            throw new Error(`Failed to create collection: ${e.message}`);
        }
    }

    /**
     * Drops a collection (table) from the database
     * @param name - The name of the collection to drop
     * @param ifExists - Whether to include IF EXISTS in the SQL statement
     * @returns Promise resolving to true if collection dropped successfully
     * @throws Error if collection drop fails
     */
    async dropCollection(name: string, ifExists = true): Promise<boolean> {
        if (!name) {
            throw new DatabaseError("Collection name is required");
        }

        const id = this.filter(name);
        const ifExistsClause = ifExists ? "IF EXISTS" : "";

        // In PostgreSQL, we can't drop multiple tables in a single statement
        // So we need to drop them one by one
        let mainTableSql = `DROP TABLE ${ifExistsClause} ${this.getSQLTable(id)}`;
        let permsTableSql = `DROP TABLE ${ifExistsClause} ${this.getSQLTable(id + "_perms")}`;

        mainTableSql = await this.trigger(
            Database.EVENT_COLLECTION_DELETE,
            mainTableSql,
        );
        permsTableSql = await this.trigger(
            Database.EVENT_COLLECTION_DELETE,
            permsTableSql,
        );

        try {
            // Drop the main table first
            await this.executeQuery(
                mainTableSql,
                [],
                undefined,
                "drop_collection_main",
            );

            // Then drop the permissions table
            await this.executeQuery(
                permsTableSql,
                [],
                undefined,
                "drop_collection_perms",
            );

            return true;
        } catch (e: any) {
            throw new Error(`Failed to drop collection: ${e.message}`);
        }
    }

    async createAttribute(
        collection: string,
        id: string,
        type: string,
        size: number,
        signed: boolean = true,
        array: boolean = false,
    ): Promise<boolean> {
        const name = this.filter(collection);
        const attrId = this.filter(id);
        const sqlType = this.getSQLType(type, size, signed, array);

        let sql = `
      ALTER TABLE ${this.getSQLTable(name)}
      ADD COLUMN "${attrId}" ${sqlType}
    `;

        sql = await this.trigger(Database.EVENT_ATTRIBUTE_CREATE, sql);

        try {
            await this.executeQuery(sql, [], undefined, "create_attribute");
            return true;
        } catch (e: any) {
            throw this.processException(e);
        }
    }

    async updateAttribute(
        collection: string,
        id: string,
        type: string,
        size: number,
        signed: boolean = true,
        array: boolean = false,
        newKey?: string,
    ): Promise<boolean> {
        const name = this.filter(collection);
        const attrId = this.filter(id);
        const newAttrId = newKey ? this.filter(newKey) : null;
        let sqlType = this.getSQLType(type, size, signed, array);

        // Special handling for TIMESTAMP
        if (sqlType === "TIMESTAMP(3)") {
            sqlType = `TIMESTAMP(3) without time zone USING TO_TIMESTAMP("${attrId}", 'YYYY-MM-DD HH24:MI:SS.MS')`;
        }

        try {
            // If a new key is provided and it's different from the current id, rename the column
            let currentId = attrId;
            if (newAttrId && attrId !== newAttrId) {
                let renameSql = `
          ALTER TABLE ${this.getSQLTable(name)}
          RENAME COLUMN "${attrId}" TO "${newAttrId}"
        `;

                renameSql = await this.trigger(
                    Database.EVENT_ATTRIBUTE_UPDATE,
                    renameSql,
                );
                await this.executeQuery(
                    renameSql,
                    [],
                    undefined,
                    "rename_attribute",
                );

                // Update currentId to the new name for the subsequent type change
                currentId = newAttrId;
            }

            // Alter the column type
            let alterSql = `
        ALTER TABLE ${this.getSQLTable(name)}
        ALTER COLUMN "${currentId}" TYPE ${sqlType}
      `;

            alterSql = await this.trigger(
                Database.EVENT_ATTRIBUTE_UPDATE,
                alterSql,
            );
            await this.executeQuery(
                alterSql,
                [],
                undefined,
                "alter_attribute_type",
            );

            return true;
        } catch (e: any) {
            throw this.processException(e);
        }
    }

    async deleteAttribute(collection: string, id: string): Promise<boolean> {
        const name = this.filter(collection);
        const attrId = this.filter(id);

        let sql = `
      ALTER TABLE ${this.getSQLTable(name)}
      DROP COLUMN "${attrId}"
    `;

        sql = await this.trigger(Database.EVENT_ATTRIBUTE_DELETE, sql);

        try {
            await this.executeQuery(sql, [], undefined, "delete_attribute");
            return true;
        } catch (e: any) {
            // PostgreSQL error code 42703 indicates column doesn't exist
            if (e.code === "42703") {
                return true;
            }
            throw this.processException(e);
        }
    }

    async renameAttribute(
        collection: string,
        oldName: string,
        newName: string,
    ): Promise<boolean> {
        const name = this.filter(collection);
        const oldAttrId = this.filter(oldName);
        const newAttrId = this.filter(newName);

        let sql = `
      ALTER TABLE ${this.getSQLTable(name)}
      RENAME COLUMN "${oldAttrId}" TO "${newAttrId}"
    `;

        sql = await this.trigger(Database.EVENT_ATTRIBUTE_UPDATE, sql);

        try {
            await this.executeQuery(sql, [], undefined, "rename_attribute");
            return true;
        } catch (e: any) {
            throw this.processException(e);
        }
    }

    async createRelationship(
        collection: string,
        relatedCollection: string,
        type: string,
        twoWay: boolean = false,
        id: string = "",
        twoWayKey: string = "",
    ): Promise<boolean> {
        const name = this.filter(collection);
        const relatedName = this.filter(relatedCollection);
        const table = this.getSQLTable(name);
        const relatedTable = this.getSQLTable(relatedName);
        const filteredId = this.filter(id);
        const filteredTwoWayKey = this.filter(twoWayKey);
        const sqlType = this.getSQLType(
            Database.VAR_RELATIONSHIP,
            0,
            false,
            false,
        );

        let sql = "";

        switch (type) {
            case Database.RELATION_ONE_TO_ONE:
                sql = `ALTER TABLE ${table} ADD COLUMN "${filteredId}" ${sqlType} DEFAULT NULL;`;

                if (twoWay) {
                    sql += `ALTER TABLE ${relatedTable} ADD COLUMN "${filteredTwoWayKey}" ${sqlType} DEFAULT NULL;`;
                }
                break;
            case Database.RELATION_ONE_TO_MANY:
                sql = `ALTER TABLE ${relatedTable} ADD COLUMN "${filteredTwoWayKey}" ${sqlType} DEFAULT NULL;`;
                break;
            case Database.RELATION_MANY_TO_ONE:
                sql = `ALTER TABLE ${table} ADD COLUMN "${filteredId}" ${sqlType} DEFAULT NULL;`;
                break;
            case Database.RELATION_MANY_TO_MANY:
                return true;
            default:
                throw new DatabaseError("Invalid relationship type");
        }

        sql = await this.trigger(Database.EVENT_ATTRIBUTE_CREATE, sql);

        try {
            await this.executeQuery(sql, [], undefined, "create_relationship");
            return true;
        } catch (e: any) {
            throw this.processException(e);
        }
    }

    async updateRelationship(
        collection: string,
        relatedCollection: string,
        type: string,
        twoWay: boolean,
        key: string,
        twoWayKey: string,
        side: string,
        newKey?: string,
        newTwoWayKey?: string,
    ): Promise<boolean> {
        const name = this.filter(collection);
        const relatedName = this.filter(relatedCollection);
        const table = this.getSQLTable(name);
        const relatedTable = this.getSQLTable(relatedName);
        const filteredKey = this.filter(key);
        const filteredTwoWayKey = this.filter(twoWayKey);

        const filteredNewKey = newKey ? this.filter(newKey) : null;
        const filteredNewTwoWayKey = newTwoWayKey
            ? this.filter(newTwoWayKey)
            : null;

        let sql = "";

        switch (type) {
            case Database.RELATION_ONE_TO_ONE:
                if (filteredKey !== filteredNewKey) {
                    sql = `ALTER TABLE ${table} RENAME COLUMN "${filteredKey}" TO "${filteredNewKey}";`;
                }
                if (twoWay && filteredTwoWayKey !== filteredNewTwoWayKey) {
                    sql += `ALTER TABLE ${relatedTable} RENAME COLUMN "${filteredTwoWayKey}" TO "${filteredNewTwoWayKey}";`;
                }
                break;
            case Database.RELATION_ONE_TO_MANY:
                if (side === Database.RELATION_SIDE_PARENT) {
                    if (filteredTwoWayKey !== filteredNewTwoWayKey) {
                        sql = `ALTER TABLE ${relatedTable} RENAME COLUMN "${filteredTwoWayKey}" TO "${filteredNewTwoWayKey}";`;
                    }
                } else {
                    if (filteredKey !== filteredNewKey) {
                        sql = `ALTER TABLE ${table} RENAME COLUMN "${filteredKey}" TO "${filteredNewKey}";`;
                    }
                }
                break;
            case Database.RELATION_MANY_TO_ONE:
                if (side === Database.RELATION_SIDE_CHILD) {
                    if (filteredTwoWayKey !== filteredNewTwoWayKey) {
                        sql = `ALTER TABLE ${relatedTable} RENAME COLUMN "${filteredTwoWayKey}" TO "${filteredNewTwoWayKey}";`;
                    }
                } else {
                    if (filteredKey !== filteredNewKey) {
                        sql = `ALTER TABLE ${table} RENAME COLUMN "${filteredKey}" TO "${filteredNewKey}";`;
                    }
                }
                break;
            case Database.RELATION_MANY_TO_MANY:
                try {
                    const collectionDoc = await this.getDocument(
                        Database.METADATA,
                        collection,
                    );
                    const relatedCollectionDoc = await this.getDocument(
                        Database.METADATA,
                        relatedCollection,
                    );

                    const junction = this.getSQLTable(
                        "_" +
                            collectionDoc.getInternalId() +
                            "_" +
                            relatedCollectionDoc.getInternalId(),
                    );

                    if (filteredNewKey) {
                        sql = `ALTER TABLE ${junction} RENAME COLUMN "${filteredKey}" TO "${filteredNewKey}";`;
                    }
                    if (twoWay && filteredNewTwoWayKey) {
                        sql += `ALTER TABLE ${junction} RENAME COLUMN "${filteredTwoWayKey}" TO "${filteredNewTwoWayKey}";`;
                    }
                } catch (e) {
                    throw this.processException(e);
                }
                break;
            default:
                throw new DatabaseError("Invalid relationship type");
        }

        if (!sql) {
            return true;
        }

        try {
            sql = await this.trigger(Database.EVENT_ATTRIBUTE_UPDATE, sql);
            await this.executeQuery(sql, [], undefined, "update_relationship");
            return true;
        } catch (e) {
            throw this.processException(e);
        }
    }

    async deleteRelationship(
        collection: string,
        relatedCollection: string,
        type: string,
        twoWay: boolean,
        key: string,
        twoWayKey: string,
        side: string,
    ): Promise<boolean> {
        const name = this.filter(collection);
        const relatedName = this.filter(relatedCollection);
        const table = this.getSQLTable(name);
        const relatedTable = this.getSQLTable(relatedName);
        const filteredKey = this.filter(key);
        const filteredTwoWayKey = this.filter(twoWayKey);

        let sql = "";

        try {
            switch (type) {
                case Database.RELATION_ONE_TO_ONE:
                    if (side === Database.RELATION_SIDE_PARENT) {
                        sql = `ALTER TABLE ${table} DROP COLUMN "${filteredKey}";`;
                        if (twoWay) {
                            sql += `ALTER TABLE ${relatedTable} DROP COLUMN "${filteredTwoWayKey}";`;
                        }
                    } else if (side === Database.RELATION_SIDE_CHILD) {
                        sql = `ALTER TABLE ${relatedTable} DROP COLUMN "${filteredTwoWayKey}";`;
                        if (twoWay) {
                            sql += `ALTER TABLE ${table} DROP COLUMN "${filteredKey}";`;
                        }
                    }
                    break;
                case Database.RELATION_ONE_TO_MANY:
                    if (side === Database.RELATION_SIDE_PARENT) {
                        sql = `ALTER TABLE ${relatedTable} DROP COLUMN "${filteredTwoWayKey}";`;
                    } else {
                        sql = `ALTER TABLE ${table} DROP COLUMN "${filteredKey}";`;
                    }
                    break;
                case Database.RELATION_MANY_TO_ONE:
                    if (side === Database.RELATION_SIDE_CHILD) {
                        sql = `ALTER TABLE ${relatedTable} DROP COLUMN "${filteredTwoWayKey}";`;
                    } else {
                        sql = `ALTER TABLE ${table} DROP COLUMN "${filteredKey}";`;
                    }
                    break;
                case Database.RELATION_MANY_TO_MANY:
                    const collectionDoc = await this.getDocument(
                        Database.METADATA,
                        collection,
                    );
                    const relatedCollectionDoc = await this.getDocument(
                        Database.METADATA,
                        relatedCollection,
                    );

                    const junction =
                        side === Database.RELATION_SIDE_PARENT
                            ? this.getSQLTable(
                                  "_" +
                                      collectionDoc.getInternalId() +
                                      "_" +
                                      relatedCollectionDoc.getInternalId(),
                              )
                            : this.getSQLTable(
                                  "_" +
                                      relatedCollectionDoc.getInternalId() +
                                      "_" +
                                      collectionDoc.getInternalId(),
                              );

                    const perms =
                        side === Database.RELATION_SIDE_PARENT
                            ? this.getSQLTable(
                                  "_" +
                                      collectionDoc.getInternalId() +
                                      "_" +
                                      relatedCollectionDoc.getInternalId() +
                                      "_perms",
                              )
                            : this.getSQLTable(
                                  "_" +
                                      relatedCollectionDoc.getInternalId() +
                                      "_" +
                                      collectionDoc.getInternalId() +
                                      "_perms",
                              );

                    sql = `DROP TABLE ${junction}; DROP TABLE ${perms}`;
                    break;
                default:
                    throw new DatabaseError("Invalid relationship type");
            }

            if (!sql) {
                return true;
            }

            sql = await this.trigger(Database.EVENT_ATTRIBUTE_DELETE, sql);
            await this.executeQuery(sql, [], undefined, "delete_relationship");
            return true;
        } catch (e: any) {
            throw this.processException(e);
        }
    }

    async createIndex(
        collection: string,
        id: string,
        type: string,
        attributes: string[],
        lengths: number[] = [],
        orders: string[] = [],
    ): Promise<boolean> {
        const collectionId = this.filter(collection);
        const indexId = this.filter(id);

        // Process attributes
        const processedAttributes: string[] = [];
        for (let i = 0; i < attributes.length; i++) {
            let attr = attributes[i];
            const order =
                !orders[i] || type === Database.INDEX_FULLTEXT ? "" : orders[i];

            // Handle special attributes
            switch (attr) {
                case "$id":
                    attr = "_uid";
                    break;
                case "$createdAt":
                    attr = "_createdAt";
                    break;
                case "$updatedAt":
                    attr = "_updatedAt";
                    break;
                default:
                    attr = this.filter(attr);
            }

            // Format attribute for SQL statement
            if (type === Database.INDEX_UNIQUE) {
                processedAttributes.push(`LOWER("${attr}"::text) ${order}`);
            } else {
                processedAttributes.push(`"${attr}" ${order}`);
            }
        }

        // Determine SQL index type
        let sqlType: string;
        switch (type) {
            case Database.INDEX_KEY:
            case Database.INDEX_FULLTEXT:
                sqlType = "INDEX";
                break;
            case Database.INDEX_UNIQUE:
                sqlType = "UNIQUE INDEX";
                break;
            default:
                throw new DatabaseError(
                    `Unknown index type: ${type}. Must be one of ${Database.INDEX_KEY}, ${Database.INDEX_UNIQUE}, ${Database.INDEX_FULLTEXT}`,
                );
        }

        const prefix = this.getPrefix();
        const key = `"${prefix}_${this.getTenantId()}_${collectionId}_${indexId}"`;
        let attributesStr = processedAttributes.join(", ");

        // Add tenant ID to index for shared tables
        if (this.sharedTables && type !== Database.INDEX_FULLTEXT) {
            attributesStr = `_tenant, ${attributesStr}`;
        }

        let sql = `CREATE ${sqlType} ${key} ON ${this.getSQLTable(collectionId)} (${attributesStr})`;
        sql = await this.trigger(Database.EVENT_INDEX_CREATE, sql);

        try {
            await this.executeQuery(sql, [], undefined, "create_index");
            return true;
        } catch (e: any) {
            throw this.processException(e);
        }
    }

    async deleteIndex(collection: string, id: string): Promise<boolean> {
        const name = this.filter(collection);
        const indexId = this.filter(id);
        const schemaName = this.database;

        const prefix = this.getPrefix();
        const key = `"${prefix}_${this.getTenantId()}_${name}_${indexId}"`;

        let sql = `DROP INDEX IF EXISTS "${schemaName}".${key}`;
        sql = await this.trigger(Database.EVENT_INDEX_DELETE, sql);

        try {
            await this.executeQuery(sql, [], undefined, "delete_index");
            return true;
        } catch (e: any) {
            throw this.processException(e);
        }
    }

    async renameIndex(
        collection: string,
        oldName: string,
        newName: string,
    ): Promise<boolean> {
        const filteredCollection = this.filter(collection);
        const filteredOldName = this.filter(oldName);
        const filteredNewName = this.filter(newName);
        const prefix = this.getPrefix();
        const schemaName = this.database;

        const oldIndexName = `"${prefix}_${this.getTenantId()}_${filteredCollection}_${filteredOldName}"`;
        const newIndexName = `"${prefix}_${this.getTenantId()}_${filteredCollection}_${filteredNewName}"`;

        let sql = `ALTER INDEX "${schemaName}".${oldIndexName} RENAME TO ${newIndexName}`;
        sql = await this.trigger(Database.EVENT_INDEX_RENAME, sql);

        try {
            await this.executeQuery(sql, [], undefined, "rename_index");
            return true;
        } catch (e: any) {
            throw this.processException(e);
        }
    }

    async createDocument(
        collection: string,
        document: Document,
    ): Promise<Document> {
        const attributes = document.getAttributes();
        attributes["_createdAt"] = document.getCreatedAt();
        attributes["_updatedAt"] = document.getUpdatedAt();
        attributes["_permissions"] = JSON.stringify(document.getPermissions());

        const name = this.filter(collection);
        let columns: string[] = [];
        let placeholders: string[] = [];
        let values: any[] = [];

        // Insert internal id if set
        if (document.getInternalId()) {
            columns.push('"_id"');
            placeholders.push("$" + (values.length + 1));
            values.push(document.getInternalId());
        }

        // Add _uid
        columns.push('"_uid"');
        placeholders.push("$" + (values.length + 1));
        values.push(document.getId());

        if (this.sharedTables) {
            columns.push("_tenant");
            placeholders.push("?");
            values.push(this.tenantId);
        }

        // Add all attributes
        for (const [attribute, value] of Object.entries(attributes)) {
            if (
                !Database.INTERNAL_ATTRIBUTES.map((v) => v.$id).includes(
                    attribute,
                )
            ) {
                const column = this.filter(attribute);
                columns.push(`"${column}"`);
                placeholders.push("$" + (values.length + 1));

                // Handle array/object values
                if (typeof value === "object" && value !== null) {
                    values.push(JSON.stringify(value));
                } else if (typeof value === "boolean") {
                    values.push(value ? "true" : "false");
                } else {
                    values.push(value);
                }
            }
        }

        let sql = `
            INSERT INTO ${this.getSQLTable(name)} (${columns.join(", ")})
            VALUES (${placeholders.join(", ")})
            RETURNING _id
        `;

        sql = await this.trigger(Database.EVENT_DOCUMENT_CREATE, sql);

        // Prepare permissions if any exist
        const permissions: string[] = [];
        const permissionValues: any[] = [];
        let permissionPlaceholderIndex = 1;

        for (const type of Object.values(Database.PERMISSIONS)) {
            for (const permission of document.getPermissionsByType(type)) {
                const cleanPermission = permission.replace(/"/g, "");

                let placeholders = [
                    `$${permissionPlaceholderIndex++}`, // type
                    `$${permissionPlaceholderIndex++}`, // permission
                    `$${permissionPlaceholderIndex++}`, // document id
                ];

                permissionValues.push(type, cleanPermission, document.getId());

                if (this.sharedTables) {
                    placeholders.push(`$${permissionPlaceholderIndex++}`);
                    permissionValues.push(
                        document.getAttribute(
                            "_tenant",
                            document.getAttribute("$tenant"),
                        ),
                    );
                }

                permissions.push(`(${placeholders.join(", ")})`);
            }
        }

        try {
            // Execute document insertion
            const result = await this.executeQuery(
                sql,
                values,
                undefined,
                "create_document",
            );
            document.setAttribute("$internalId", result.rows[0]._id);

            // Execute permissions insertion if needed
            if (permissions.length > 0) {
                const sqlTenant = this.sharedTables ? ", _tenant" : "";

                const queryPermissions = `
                    INSERT INTO ${this.getSQLTable(name + "_perms")} (_type, _permission, _document${sqlTenant})
                    VALUES ${permissions.join(", ")}
                `;

                const triggeredPermissionsQuery = await this.trigger(
                    Database.EVENT_PERMISSIONS_CREATE,
                    queryPermissions,
                );

                await this.executeQuery(
                    triggeredPermissionsQuery,
                    permissionValues,
                    undefined,
                    "create_document_permissions",
                );
            }

            return document;
        } catch (e: any) {
            throw this.processException(e);
        }
    }

    async createDocuments(
        collection: string,
        documents: Document[],
        batchSize: number = 500,
    ): Promise<Document[]> {
        if (documents.length === 0) {
            return documents;
        }

        try {
            // Process in batches if needed
            if (documents.length > batchSize) {
                const results: Document[] = [];
                for (let i = 0; i < documents.length; i += batchSize) {
                    const batch = documents.slice(i, i + batchSize);
                    const batchResults = await this.createDocuments(
                        collection,
                        batch,
                        batchSize,
                    );
                    results.push(...batchResults);
                }
                return results;
            }

            const name = this.filter(collection);
            const attributeKeys = new Set([
                "_uid",
                "_createdAt",
                "_updatedAt",
                "_permissions",
            ]);

            // Check internal ID consistency
            let hasInternalId: boolean | null = null;
            for (const document of documents) {
                Object.keys(document.getAttributes())
                    .filter(
                        (attr) =>
                            !Database.INTERNAL_ATTRIBUTES.map(
                                (v) => v.$id,
                            ).includes(attr),
                    )
                    .forEach((key) => attributeKeys.add(key));

                if (hasInternalId === null) {
                    hasInternalId = !!document.getInternalId();
                } else if (hasInternalId !== !!document.getInternalId()) {
                    throw new DatabaseError(
                        "All documents must have an internalId if one is set",
                    );
                }
            }

            if (this.sharedTables) {
                attributeKeys.add("_tenant");
            }

            if (hasInternalId) {
                attributeKeys.add("_id");
            }

            const attributeArray = Array.from(attributeKeys);
            const columns = attributeArray.map(
                (attr) => `"${this.filter(attr)}"`,
            );

            const placeholders: string[] = [];
            const values: any[] = [];
            const permissionValues: any[] = [];
            let permissionPlaceholderIndex = 1;
            const permissionStatements: string[] = [];
            const internalIds: Record<string, boolean> = {};

            // Prepare document data
            for (const document of documents) {
                const docPlaceholders: string[] = [];
                const attributes: Record<string, any> = {
                    ...document.getAttributes(),
                };

                // Add standard fields
                attributes["_uid"] = document.getId();
                attributes["_createdAt"] = document.getCreatedAt();
                attributes["_updatedAt"] = document.getUpdatedAt();
                attributes["_permissions"] = JSON.stringify(
                    document.getPermissions(),
                );

                if (document.getInternalId()) {
                    internalIds[document.getId()] = true;
                    attributes["_id"] = document.getInternalId();
                }

                if (this.sharedTables) {
                    attributes["_tenant"] = document.getAttribute(
                        "_tenant",
                        document.getAttribute("$tenant"),
                    );
                }

                // Prepare placeholders and values
                for (const key of attributeArray) {
                    let value = attributes[key] ?? null;

                    if (typeof value === "object" && value !== null) {
                        value = JSON.stringify(value);
                    } else if (typeof value === "boolean") {
                        value = value ? "true" : "false";
                    }

                    docPlaceholders.push(`$${values.length + 1}`);
                    values.push(value);
                }

                placeholders.push(`(${docPlaceholders.join(", ")})`);

                // Prepare permissions data
                for (const type of Object.values(Database.PERMISSIONS)) {
                    for (const permission of document.getPermissionsByType(
                        type,
                    )) {
                        const cleanPermission = permission.replace(/"/g, "");

                        let permPlaceholders = [
                            `$${permissionPlaceholderIndex++}`, // type
                            `$${permissionPlaceholderIndex++}`, // permission
                            `$${permissionPlaceholderIndex++}`, // document id
                        ];

                        permissionValues.push(
                            type,
                            cleanPermission,
                            document.getId(),
                        );

                        if (this.sharedTables) {
                            permPlaceholders.push(
                                `$${permissionPlaceholderIndex++}`,
                            );
                            permissionValues.push(
                                document.getAttribute(
                                    "_tenant",
                                    document.getAttribute("$tenant"),
                                ),
                            );
                        }

                        permissionStatements.push(
                            `(${permPlaceholders.join(", ")})`,
                        );
                    }
                }
            }

            // Execute document insert
            let sql = `
                INSERT INTO ${this.getSQLTable(name)} (${columns.join(", ")})
                VALUES ${placeholders.join(", ")}
                RETURNING _id, _uid
            `;

            sql = await this.trigger(Database.EVENT_DOCUMENT_CREATE, sql);
            const result = await this.executeQuery(
                sql,
                values,
                undefined,
                "create_documents",
            );
            const idMap = new Map<string, number>();
            for (const row of result.rows) {
                idMap.set(row._uid, row._id);
            }

            for (const document of documents) {
                if (!internalIds[document.getId()]) {
                    const internalId = idMap.get(document.getId());
                    if (internalId) {
                        document.setAttribute("$internalId", internalId);
                    }
                }
            }

            // Insert permissions if any exist
            if (permissionStatements.length > 0) {
                const sqlTenant = this.sharedTables ? ", _tenant" : "";

                const permSql = `
                    INSERT INTO ${this.getSQLTable(name + "_perms")} (_type, _permission, _document${sqlTenant})
                    VALUES ${permissionStatements.join(", ")}
                `;

                const triggeredPermSql = await this.trigger(
                    Database.EVENT_PERMISSIONS_CREATE,
                    permSql,
                );

                await this.executeQuery(
                    triggeredPermSql,
                    permissionValues,
                    undefined,
                    "create_documents_permissions",
                );
            }

            return documents;
        } catch (e: any) {
            throw this.processException(e);
        }
    }

    /**
     * Retrieves the current permissions for a given document.
     *
     * @param name - The name of the entity for which permissions are being retrieved.
     * @param documentId - The ID of the document for which permissions are being retrieved.
     * @param tenantId - Optional tenant ID for shared tables.
     * @returns A promise that resolves to an object where the keys are permission types and the values are arrays of permissions.
     */
    private async getCurrentPermissions(
        name: string,
        documentId: string,
        tenantId?: any,
    ): Promise<Record<string, string[]>> {
        const permsTableName = this.getSQLTable(name + "_perms");
        const params = [documentId];

        let sql = `SELECT _type, _permission FROM ${permsTableName} WHERE _document = $1`;

        if (this.sharedTables) {
            sql += " AND _tenant = $2";
            params.push(tenantId);
        }

        sql = await this.trigger(Database.EVENT_PERMISSIONS_READ, sql);

        const result = await this.executeQuery(
            sql,
            params,
            undefined,
            "get_current_permissions",
        );

        // Organize permissions by type
        const permissions: Record<string, string[]> = {};
        for (const type of Object.values(Database.PERMISSIONS)) {
            permissions[type] = [];
        }

        for (const row of result.rows) {
            if (!permissions[row._type]) {
                permissions[row._type] = [];
            }
            permissions[row._type].push(row._permission);
        }

        return permissions;
    }

    /**
     * Computes the changes in permissions by comparing the current permissions with the new permissions.
     *
     * @param currentPermissions - An object representing the current permissions, where keys are permission types and values are arrays of resources.
     * @param document - The document with the new permissions to compare against.
     * @returns An object containing two properties: additions and removals.
     */
    private getPermissionChanges(
        currentPermissions: Record<string, string[]>,
        document: Document,
    ): {
        additions: Record<string, string[]>;
        removals: Record<string, string[]>;
    } {
        const additions: Record<string, string[]> = {};
        const removals: Record<string, string[]> = {};

        // Initialize additions and removals with empty arrays for each permission type
        for (const type of Object.values(Database.PERMISSIONS)) {
            additions[type] = [];
            removals[type] = [];
        }

        // Find permissions to add
        for (const type of Object.values(Database.PERMISSIONS)) {
            const typedPermissions = currentPermissions[type] || [];
            const newPermissions = document.getPermissionsByType(type);

            additions[type] = newPermissions.filter(
                (perm) => !typedPermissions.includes(perm),
            );
        }

        // Find permissions to remove
        for (const type of Object.values(Database.PERMISSIONS)) {
            const typedPermissions = currentPermissions[type] || [];
            const newPermissions = document.getPermissionsByType(type);

            removals[type] = typedPermissions.filter(
                (perm) => !newPermissions.includes(perm),
            );
        }

        return { additions, removals };
    }

    /**
     * Removes permissions from a specified document in the database.
     *
     * @param name - The name of the table (without the '_perms' suffix) from which permissions will be removed.
     * @param documentId - The ID of the document whose permissions are to be removed.
     * @param removals - An object where keys are permission types and values are arrays of permissions to be removed.
     * @param tenantId - Optional tenant ID for shared tables.
     * @returns A promise that resolves when the permissions have been removed.
     */
    private async removePermissions(
        name: string,
        documentId: string,
        removals: Record<string, string[]>,
        tenantId?: any,
    ): Promise<void> {
        const permsTableName = this.getSQLTable(name + "_perms");

        // Skip if nothing to remove
        if (Object.values(removals).every((perms) => perms.length === 0)) {
            return;
        }

        let sql = `DELETE FROM ${permsTableName} WHERE _document = $1`;
        const params: any[] = [documentId];
        let paramIndex = 2;

        if (this.sharedTables) {
            sql += " AND _tenant = $2";
            params.push(tenantId);
            paramIndex++;
        }

        const conditions: string[] = [];

        for (const [type, perms] of Object.entries(removals)) {
            if (perms.length === 0) continue;

            const permPlaceholders = perms.map(() => `$${paramIndex++}`);
            conditions.push(
                `(_type = '${type}' AND _permission IN (${permPlaceholders.join(", ")}))`,
            );
            params.push(...perms);
        }

        if (conditions.length > 0) {
            sql += ` AND (${conditions.join(" OR ")})`;
        } else {
            return; // Nothing to delete
        }

        sql = await this.trigger(Database.EVENT_PERMISSIONS_DELETE, sql);
        await this.executeQuery(sql, params, undefined, "remove_permissions");
    }

    /**
     * Adds permissions to a specified document in the database.
     *
     * @param name - The name of the collection or table to which permissions are being added.
     * @param documentId - The ID of the document to which permissions are being added.
     * @param additions - An object containing the types of permissions and their corresponding values to be added.
     * @param tenantId - Optional tenant ID for shared tables.
     * @returns A promise that resolves when the permissions have been successfully added to the database.
     */
    private async addPermissions(
        name: string,
        documentId: string,
        additions: Record<string, string[]>,
        tenantId?: any,
    ): Promise<void> {
        const permsTableName = this.getSQLTable(name + "_perms");

        // Skip if nothing to add
        if (Object.values(additions).every((perms) => perms.length === 0)) {
            return;
        }

        const valueGroups: string[] = [];
        const params: any[] = [];
        let paramIndex = 1;

        for (const [type, perms] of Object.entries(additions)) {
            for (const perm of perms) {
                const valueItems = [
                    `$${paramIndex++}`,
                    `$${paramIndex++}`,
                    `$${paramIndex++}`,
                ];
                params.push(documentId, type, perm);

                if (this.sharedTables) {
                    valueItems.push(`$${paramIndex++}`);
                    params.push(tenantId);
                }

                valueGroups.push(`(${valueItems.join(", ")})`);
            }
        }

        if (valueGroups.length === 0) {
            return; // Nothing to insert
        }

        const tenantCol = this.sharedTables ? ", _tenant" : "";
        let sql = `
            INSERT INTO ${permsTableName} (_document, _type, _permission${tenantCol})
            VALUES ${valueGroups.join(", ")}
        `;

        sql = await this.trigger(Database.EVENT_PERMISSIONS_CREATE, sql);
        await this.executeQuery(sql, params);
    }

    async updateDocument(
        collection: string,
        id: string,
        document: Document,
    ): Promise<Document> {
        const name = this.filter(collection);
        const tableName = this.getSQLTable(name);

        if (!id) throw new DatabaseError("Document ID is required.");

        const tenantId = document.getAttribute(
            "_tenant",
            document.getAttribute("$tenant"),
        );

        try {
            // Get current permissions
            const currentPermissions = await this.getCurrentPermissions(
                name,
                id,
                tenantId,
            );

            // Calculate permission changes
            const { additions, removals } = this.getPermissionChanges(
                currentPermissions,
                document,
            );

            // Remove permissions that should be removed
            await this.removePermissions(name, id, removals, tenantId);

            // Add new permissions
            await this.addPermissions(
                name,
                document.getId(),
                additions,
                tenantId,
            );

            // Update document attributes
            const attributes = document.getAttributes();
            if (document.getCreatedAt()) {
                attributes._createdAt = document.getCreatedAt();
            }
            if (document.getUpdatedAt()) {
                attributes._updatedAt = document.getUpdatedAt();
            }
            if (document.getPermissions()) {
                attributes._permissions = document.getPermissions();
            }

            const setClauses: string[] = [];
            const updateParams: any[] = [document.getId(), id]; // [new uid, existing uid]
            let paramIndex = 3;

            if (this.sharedTables) {
                attributes._tenant = this.tenantId;
                updateParams.push(this.tenantId);
            }

            for (const [attr, value] of Object.entries(attributes)) {
                if (
                    !Database.INTERNAL_ATTRIBUTES.map((v) => v.$id).includes(
                        attr,
                    )
                ) {
                    const column = this.filter(attr);
                    setClauses.push(`"${column}" = $${paramIndex++}`);

                    if (typeof value === "object" && value !== null) {
                        updateParams.push(JSON.stringify(value));
                    } else if (typeof value === "boolean") {
                        updateParams.push(value ? "true" : "false");
                    } else {
                        updateParams.push(value);
                    }
                }
            }

            const tenantCondition = this.sharedTables ? "AND _tenant = $3" : "";
            let updateSql = `
                UPDATE ${tableName}
                SET ${setClauses.join(", ")}, _uid = $1
                WHERE _uid = $2 ${tenantCondition}
            `;

            updateSql = await this.trigger(
                Database.EVENT_DOCUMENT_UPDATE,
                updateSql,
            );
            await this.executeQuery(updateSql, updateParams);

            return document;
        } catch (e: any) {
            throw this.processException(e);
        }
    }

    async updateDocuments(
        collection: string,
        updates: Document,
        documents: Document[],
    ): Promise<number> {
        if (documents.length === 0) {
            return 0;
        }

        const attributes = updates.getAttributes();

        if (updates.getUpdatedAt()) {
            attributes["_updatedAt"] = updates.getUpdatedAt();
        }

        if (updates.getPermissions()) {
            attributes["_permissions"] = JSON.stringify(
                updates.getPermissions(),
            );
        }

        if (Object.keys(attributes).length === 0) {
            return 0;
        }

        const name = this.filter(collection);
        const ids = documents.map((doc) => doc.getId());

        // Prepare update columns and values
        const updateColumns: string[] = [];
        const values: any[] = [];
        let paramIndex = 1;

        for (const attr of Object.keys(attributes)) {
            if (
                !Database.INTERNAL_ATTRIBUTES.map((v) => v.$id).includes(attr)
            ) {
                updateColumns.push(`"${this.filter(attr)}" = $${paramIndex}`);

                const value = attributes[attr];
                if (
                    Array.isArray(value) ||
                    (typeof value === "object" && value !== null)
                ) {
                    values.push(JSON.stringify(value));
                } else if (typeof value === "boolean") {
                    values.push(value ? "true" : "false");
                } else {
                    values.push(value);
                }

                paramIndex++;
            }
        }

        // Prepare where clause for document IDs
        const idPlaceholders = ids.map((_, i) => `$${paramIndex + i}`);
        const whereClause = [`_uid IN (${idPlaceholders.join(", ")})`];
        values.push(...ids);

        // Add tenant condition if using shared tables
        if (this.sharedTables) {
            whereClause.push(`_tenant = $${paramIndex + ids.length}`);
            values.push(
                updates.getAttribute(
                    "_tenant",
                    updates.getAttribute("$tenant"),
                ),
            );
        }

        let sql = `
            UPDATE ${this.getSQLTable(name)}
            SET ${updateColumns.join(", ")}
            WHERE ${whereClause.join(" AND ")}
        `;

        sql = await this.trigger(Database.EVENT_DOCUMENTS_UPDATE, sql);

        try {
            const result = await this.executeQuery(sql, values);
            const affected = result.rowCount;

            // Update permissions if needed
            if (updates.getPermissions()) {
                for (const document of documents) {
                    const tenantId = document.getAttribute(
                        "_tenant",
                        document.getAttribute("$tenant"),
                    );

                    // Get current permissions for the document
                    const currentPermissions = await this.getCurrentPermissions(
                        name,
                        document.getId(),
                        tenantId,
                    );

                    // Determine which permissions to add or remove
                    const { additions, removals } = this.getPermissionChanges(
                        currentPermissions,
                        updates,
                    );

                    // Remove permissions that should be removed
                    if (
                        Object.values(removals).some(
                            (perms) => perms.length > 0,
                        )
                    ) {
                        await this.removePermissions(
                            name,
                            document.getId(),
                            removals,
                            tenantId,
                        );
                    }

                    // Add new permissions
                    if (
                        Object.values(additions).some(
                            (perms) => perms.length > 0,
                        )
                    ) {
                        await this.addPermissions(
                            name,
                            document.getId(),
                            additions,
                            tenantId,
                        );
                    }
                }
            }

            return affected!;
        } catch (e: any) {
            throw this.processException(e);
        }
    }

    async increaseDocumentAttribute(
        collection: string,
        id: string,
        attribute: string,
        value: number,
        updatedAt: string,
        min?: number,
        max?: number,
    ): Promise<boolean> {
        const name = this.filter(collection);
        const filteredAttribute = this.filter(attribute);
        const values: any[] = [];
        let paramIndex = 1;

        let sql = `
            UPDATE ${this.getSQLTable(name)}
            SET "${filteredAttribute}" = 
                CASE 
                    WHEN $${paramIndex}::numeric IS NOT NULL AND "${filteredAttribute}" + $${paramIndex + 1}::numeric > $${paramIndex + 2}::numeric THEN $${paramIndex + 3}::numeric
                    WHEN $${paramIndex + 4}::numeric IS NOT NULL AND "${filteredAttribute}" + $${paramIndex + 5}::numeric < $${paramIndex + 6}::numeric THEN $${paramIndex + 7}::numeric
                    ELSE "${filteredAttribute}" + $${paramIndex + 8}::numeric
                END,
            "_updatedAt" = $${paramIndex + 9}
            WHERE _uid = $${paramIndex + 10}
        `;

        // Add values for CASE conditions - explicitly cast null values to avoid PostgreSQL type inference issues
        values.push(max ?? null); // WHEN max is defined
        values.push(value);
        values.push(max ?? null);
        values.push(max ?? null); // Enforce max limit

        values.push(min ?? null); // WHEN min is defined
        values.push(value);
        values.push(min ?? null);
        values.push(min ?? null); // Enforce min limit

        // Default increment
        values.push(value);
        values.push(updatedAt);
        values.push(id);

        paramIndex += 11;

        if (this.sharedTables) {
            sql += ` AND (_tenant = $${paramIndex} OR _tenant IS NULL)`;
            values.push(this.getTenantId());
        }

        sql = await this.trigger(Database.EVENT_DOCUMENT_UPDATE, sql);

        try {
            const result = await this.executeQuery(sql, values);
            return (result.rowCount ?? 0) > 0;
        } catch (e: any) {
            throw this.processException(e);
        }
    }

    async deleteDocument(collection: string, uid: string): Promise<boolean> {
        const name = this.filter(collection);
        const table = this.getSQLTable(name);
        const permsTable = this.getSQLTable(name + "_perms");

        // Create parameterized queries
        let params: any[] = [uid];
        let sql = `DELETE FROM ${table} WHERE _uid = $1`;

        // Add tenant condition if using shared tables
        if (this.sharedTables) {
            sql += ` AND _tenant = $2`;
            params.push(this.getTenantId());
        }

        // Apply trigger for document deletion
        sql = await this.trigger(Database.EVENT_DOCUMENT_DELETE, sql);

        // Create permissions deletion query
        let permsParams: any[] = [uid];
        let permsSql = `DELETE FROM ${permsTable} WHERE _document = $1`;

        // Add tenant condition for permissions if using shared tables
        if (this.sharedTables) {
            permsSql += ` AND _tenant = $2`;
            permsParams.push(this.getTenantId());
        }

        // Apply trigger for permissions deletion
        permsSql = await this.trigger(
            Database.EVENT_PERMISSIONS_DELETE,
            permsSql,
        );

        try {
            // Execute document deletion
            const result = await this.executeQuery(sql, params);
            const deleted = result.rowCount! > 0;

            // Execute permissions deletion regardless of whether document existed
            await this.executeQuery(permsSql, permsParams);

            return deleted;
        } catch (e: any) {
            throw this.processException(e);
        }
    }

    async deleteDocuments(collection: string, ids: string[]): Promise<number> {
        if (ids.length === 0) {
            return 0;
        }

        const name = this.filter(collection);
        const table = this.getSQLTable(name);
        const permsTable = this.getSQLTable(name + "_perms");

        // Create parameterized query
        const placeholders = ids.map((_, i) => `$${i + 1}`).join(", ");
        const params = [...ids];
        let paramIndex = ids.length + 1;

        let sql = `DELETE FROM ${table} WHERE _uid IN (${placeholders})`;

        // Add tenant condition if using shared tables
        if (this.sharedTables) {
            sql += ` AND _tenant = $${paramIndex}`;
            params.push(this.getTenantId() as any);
            paramIndex++;
        }

        // Apply trigger for document deletion
        sql = await this.trigger(Database.EVENT_DOCUMENT_DELETE, sql);

        // Create permissions deletion query
        let permsSql = `DELETE FROM ${permsTable} WHERE _document IN (${placeholders})`;

        // Add tenant condition for permissions if using shared tables
        if (this.sharedTables) {
            permsSql += ` AND _tenant = $${ids.length + 1}`;
        }

        // Apply trigger for permissions deletion
        permsSql = await this.trigger(
            Database.EVENT_PERMISSIONS_DELETE,
            permsSql,
        );

        try {
            // Execute document deletion
            const result = await this.executeQuery(sql, params);
            const deleted = result.rowCount || 0;

            // Execute permissions deletion regardless of whether documents existed
            await this.executeQuery(
                permsSql,
                this.sharedTables ? [...ids, this.getTenantId()] : ids,
            );

            return deleted;
        } catch (e: any) {
            throw this.processException(e);
        }
    }

    async find(
        collection: string,
        queries: Query[] = [],
        limit: number = 25,
        offset: number | null = 0,
        orderAttributes: string[] = [],
        orderTypes: string[] = [],
        cursor: any = {},
        cursorDirection: "after" | "before" = Database.CURSOR_AFTER,
        forPermission: string = Database.PERMISSION_READ,
    ): Promise<Document[]> {
        const name = this.filter(collection);
        const roles = Authorization.getRoles();
        const where: string[] = [];
        const orders: string[] = [];
        const params: any[] = [];
        let paramIndex = 1;

        // Clone queries to prevent side effects
        queries = queries.map((query) =>
            Object.assign(Object.create(Object.getPrototypeOf(query)), query),
        );

        // Map special attributes in order fields
        const mappedOrderAttributes = orderAttributes.map((attr) => {
            switch (attr) {
                case "$id":
                    return "_uid";
                case "$internalId":
                    return "_id";
                case "$tenant":
                    return "_tenant";
                case "$createdAt":
                    return "_createdAt";
                case "$updatedAt":
                    return "_updatedAt";
                default:
                    return attr;
            }
        });

        // Check for ID attribute in ordering
        let hasIdAttribute = false;
        for (let i = 0; i < mappedOrderAttributes.length; i++) {
            const attribute = mappedOrderAttributes[i];
            if (["_uid", "_id"].includes(attribute)) {
                hasIdAttribute = true;
            }

            const filterAttribute = this.filter(attribute);
            let orderType = this.filter(orderTypes[i] || Database.ORDER_ASC);

            // Handle cursor-based pagination for first order attribute
            if (
                i === 0 &&
                cursor &&
                cursor[
                    attribute === "_uid"
                        ? "$id"
                        : attribute === "_id"
                          ? "$internalId"
                          : attribute === "_tenant"
                            ? "$tenant"
                            : attribute === "_createdAt"
                              ? "$createdAt"
                              : attribute === "_updatedAt"
                                ? "$updatedAt"
                                : attribute
                ]
            ) {
                let orderMethodInternalId = Query.TYPE_GREATER; // To preserve natural order
                let orderMethod =
                    orderType === Database.ORDER_DESC
                        ? Query.TYPE_LESSER
                        : Query.TYPE_GREATER;

                if (cursorDirection === Database.CURSOR_BEFORE) {
                    orderType =
                        orderType === Database.ORDER_ASC
                            ? Database.ORDER_DESC
                            : Database.ORDER_ASC;
                    orderMethodInternalId =
                        orderType === Database.ORDER_ASC
                            ? Query.TYPE_LESSER
                            : Query.TYPE_GREATER;
                    orderMethod =
                        orderType === Database.ORDER_DESC
                            ? Query.TYPE_LESSER
                            : Query.TYPE_GREATER;
                }

                where.push(`(
                    table_main."${filterAttribute}" ${this.getSQLOperator(orderMethod)} $${paramIndex} 
                    OR (
                        table_main."${filterAttribute}" = $${paramIndex} 
                        AND
                        table_main._id ${this.getSQLOperator(orderMethodInternalId)} $${paramIndex + 1}
                    )
                )`);

                // Get cursor attribute key
                const cursorAttrKey =
                    attribute === "_uid"
                        ? "$id"
                        : attribute === "_id"
                          ? "$internalId"
                          : attribute === "_tenant"
                            ? "$tenant"
                            : attribute === "_createdAt"
                              ? "$createdAt"
                              : attribute === "_updatedAt"
                                ? "$updatedAt"
                                : attribute;

                params.push(cursor[cursorAttrKey]);
                params.push(cursor["$internalId"]);
                paramIndex += 2;
            } else if (cursorDirection === Database.CURSOR_BEFORE) {
                orderType =
                    orderType === Database.ORDER_ASC
                        ? Database.ORDER_DESC
                        : Database.ORDER_ASC;
            }

            orders.push(`table_main."${filterAttribute}" ${orderType}`);
        }

        // Allow after pagination without any order
        if (
            mappedOrderAttributes.length === 0 &&
            cursor &&
            cursor["$internalId"]
        ) {
            const orderType = orderTypes[0] || Database.ORDER_ASC;
            const orderMethod =
                cursorDirection === Database.CURSOR_AFTER
                    ? orderType === Database.ORDER_DESC
                        ? Query.TYPE_LESSER
                        : Query.TYPE_GREATER
                    : orderType === Database.ORDER_DESC
                      ? Query.TYPE_GREATER
                      : Query.TYPE_LESSER;

            where.push(
                `(table_main._id ${this.getSQLOperator(orderMethod)} $${paramIndex})`,
            );
            params.push(cursor["$internalId"]);
            paramIndex++;
        }

        // Add natural order if needed
        if (!hasIdAttribute) {
            if (mappedOrderAttributes.length === 0 && orderTypes.length > 0) {
                let order = orderTypes[0] || Database.ORDER_ASC;
                if (cursorDirection === Database.CURSOR_BEFORE) {
                    order =
                        order === Database.ORDER_ASC
                            ? Database.ORDER_DESC
                            : Database.ORDER_ASC;
                }
                orders.push(`table_main._id ${this.filter(order)}`);
            } else {
                orders.push(
                    `table_main._id ${cursorDirection === Database.CURSOR_AFTER ? Database.ORDER_ASC : Database.ORDER_DESC}`,
                );
            }
        }

        // Process query conditions
        if (queries && queries.length > 0) {
            const conditionsAndParams = this.getSQLConditionsWithParams(
                queries,
                paramIndex,
            );
            if (conditionsAndParams.conditions) {
                where.push(conditionsAndParams.conditions);
                params.push(...conditionsAndParams.params);
                paramIndex += conditionsAndParams.params.length;
            }
        }

        // Add tenant condition for shared tables
        if (this.sharedTables) {
            let orIsNull = "";
            if (collection === Database.METADATA) {
                orIsNull = " OR table_main._tenant IS NULL";
            }
            where.push(`(table_main._tenant = $${paramIndex}${orIsNull})`);
            params.push(this.getTenantId());
            paramIndex++;
        }

        // Add authorization check if enabled
        if (Authorization.getStatus()) {
            where.push(
                this.getSQLPermissionsConditionWithParam(
                    name,
                    roles,
                    forPermission,
                    paramIndex,
                ),
            );
            if (this.sharedTables) {
                params.push(this.getTenantId());
                paramIndex++;
            }
        }

        // Get attribute selections if applicable
        const selections = this.getAttributeSelections(queries);
        const attributeProjection = this.getAttributeProjection(
            selections,
            "table_main",
        );

        // Build SQL query
        const sqlWhere = where.length > 0 ? `WHERE ${where.join(" AND ")}` : "";
        const sqlOrder =
            orders.length > 0 ? `ORDER BY ${orders.join(", ")}` : "";
        const sqlLimit = limit !== null ? `LIMIT $${paramIndex++}` : "";
        const sqlOffset = offset !== null ? `OFFSET $${paramIndex++}` : "";

        let sql = `
            SELECT ${attributeProjection}
            FROM ${this.getSQLTable(name)} as table_main
            ${sqlWhere}
            ${sqlOrder}
            ${sqlLimit}
            ${sqlOffset}
        `;

        sql = await this.trigger(Database.EVENT_DOCUMENT_FIND, sql);

        try {
            // Add limit and offset to params if needed
            if (limit !== null) {
                params.push(limit);
            }
            if (offset !== null) {
                params.push(offset);
            }

            // Execute query
            const result = await this.executeQuery(sql, params);
            const documents: Document[] = [];

            // Transform rows to documents
            for (const row of result.rows) {
                documents.push(this.objectToDocument(row));
            }

            // Reverse results for "before" cursor direction
            if (cursorDirection === Database.CURSOR_BEFORE) {
                documents.reverse();
            }

            return documents;
        } catch (e: any) {
            console.error(`SQL Query: ${sql}`);
            console.error(`Params: ${JSON.stringify(params)}`);
            throw this.processException(e);
        }
    }

    async count(
        collection: string,
        queries: Query[] = [],
        max: number | null = null,
    ): Promise<number> {
        const name = this.filter(collection);
        const roles = Authorization.getRoles();
        const where: string[] = [];
        const params: any[] = [];
        let paramIndex = 1;

        // Clone queries to prevent side effects
        queries = queries.map((query) =>
            Object.assign(Object.create(Object.getPrototypeOf(query)), query),
        );

        // Process query conditions
        if (queries && queries.length > 0) {
            const conditions = this.getSQLConditions(queries);
            if (conditions) {
                where.push(conditions);
                let conditionParams: any[] = [];
                for (const query of queries) {
                    this.bindConditionValue(conditionParams, query);
                }
                params.push(...conditionParams);
                paramIndex += conditionParams.length;
            }
        }

        // Add tenant condition for shared tables
        if (this.sharedTables) {
            let orIsNull = "";
            if (collection === Database.METADATA) {
                orIsNull = " OR table_main._tenant IS NULL";
            }
            where.push(`(table_main._tenant = $${paramIndex++}${orIsNull})`);
            params.push(this.getTenantId());
        }

        // Add authorization check if enabled
        if (Authorization.getStatus()) {
            where.push(this.getSQLPermissionsCondition(name, roles));
            if (this.sharedTables) {
                params.push(this.getTenantId());
                paramIndex++;
            }
        }

        const sqlWhere = where.length > 0 ? `WHERE ${where.join(" AND ")}` : "";
        const limit = max === null ? "" : `LIMIT $${paramIndex++}`;

        let sql = `
            SELECT COUNT(1) as sum FROM (
                SELECT 1
                FROM ${this.getSQLTable(name)} table_main
                ${sqlWhere}
                ${limit}
            ) table_count
        `;

        sql = await this.trigger(Database.EVENT_DOCUMENT_COUNT, sql);

        try {
            // Add max parameter if needed
            if (max !== null) {
                params.push(max);
            }

            const result = await this.executeQuery(sql, params);
            return parseInt(result.rows[0].sum) || 0;
        } catch (e: any) {
            throw this.processException(e);
        }
    }

    async sum(
        collection: string,
        attribute: string,
        queries: Query[] = [],
        max: number | null = null,
    ): Promise<number> {
        const name = this.filter(collection);
        const filteredAttribute = this.filter(attribute);
        const roles = Authorization.getRoles();
        const where: string[] = [];
        const params: any[] = [];
        let paramIndex = 1;

        // Clone queries to prevent side effects
        queries = queries.map((query) =>
            Object.assign(Object.create(Object.getPrototypeOf(query)), query),
        );

        // Process query conditions
        if (queries && queries.length > 0) {
            const conditions = this.getSQLConditions(queries);
            if (conditions) {
                where.push(conditions);
                let conditionParams: any[] = [];
                for (const query of queries) {
                    this.bindConditionValue(conditionParams, query);
                }
                params.push(...conditionParams);
                paramIndex += conditionParams.length;
            }
        }

        // Add tenant condition for shared tables
        if (this.sharedTables) {
            let orIsNull = "";
            if (collection === Database.METADATA) {
                orIsNull = " OR table_main._tenant IS NULL";
            }
            where.push(`(table_main._tenant = $${paramIndex}${orIsNull})`);
            params.push(this.getTenantId());
            paramIndex++;
        }

        // Add authorization check if enabled
        if (Authorization.getStatus()) {
            where.push(this.getSQLPermissionsCondition(name, roles));
            if (this.sharedTables) {
                params.push(this.getTenantId());
                paramIndex++;
            }
        }

        const sqlWhere = where.length > 0 ? `WHERE ${where.join(" AND ")}` : "";
        const limit = max === null ? "" : `LIMIT $${paramIndex++}`;

        let sql = `
            SELECT SUM("${filteredAttribute}") as sum FROM (
                SELECT "${filteredAttribute}"
                FROM ${this.getSQLTable(name)} table_main
                ${sqlWhere}
                ${limit}
            ) table_count
        `;

        sql = await this.trigger(Database.EVENT_DOCUMENT_SUM, sql);

        try {
            // Add max parameter if needed
            if (max !== null) {
                params.push(max);
            }

            const result = await this.executeQuery(sql, params);
            return parseFloat(result.rows[0].sum) || 0;
        } catch (e: any) {
            throw this.processException(e);
        }
    }

    async getDocument(
        collection: string,
        uid: string,
        queries: Query[] = [],
        forUpdate: boolean = false,
    ): Promise<Document> {
        const name = this.filter(collection);
        const selections = this.getAttributeSelections(queries);
        const params: any[] = [uid];
        let paramIndex = 2;

        let sql = `
            SELECT ${this.getAttributeProjection(selections, "table_main")}
            FROM ${this.getSQLTable(name)} AS table_main
            WHERE _uid = $1
            ${this.sharedTables ? `AND (_tenant = $${paramIndex++} OR _tenant IS NULL)` : ""}
            ${forUpdate ? "FOR UPDATE" : ""}
        `;

        if (this.sharedTables) {
            params.push(this.getTenantId());
        }

        sql = await this.trigger(Database.EVENT_DOCUMENT_FIND, sql);

        try {
            const result = await this.executeQuery(sql, params);

            return this.objectToDocument(result.rows[0]);
        } catch (e: any) {
            throw this.processException(e);
        }
    }

    async getDocuments(
        collection: string,
        limit: number = 25,
        offset: number = 0,
    ): Promise<Document[]> {
        try {
            // Use the existing find method with empty queries to get all documents
            return await this.find(
                collection,
                [], // No specific queries
                limit, // Use provided limit or default
                offset, // Use provided offset or default
                ["$updatedAt"], // Order by update time
                [Database.ORDER_DESC], // Most recent first
            );
        } catch (e: any) {
            throw this.processException(e);
        }
    }

    async getConnectionId(): Promise<number> {
        try {
            const result = await this.executeQuery(
                "SELECT pg_backend_pid() as pid",
            );
            return result.rows[0].pid;
        } catch (e: any) {
            throw new DatabaseError(
                `Failed to get connection ID: ${e.message}`,
            );
        }
    }

    async getSizeOfCollection(collection: string): Promise<number> {
        const filteredCollection = this.filter(collection);
        const tableName = this.getSQLTable(filteredCollection);
        const permissionsTable = this.getSQLTable(
            filteredCollection + "_perms",
        );

        try {
            // Get size of main collection table
            const collectionSizeResult = await this.executeQuery(
                `SELECT pg_relation_size($1) as size`,
                [tableName],
            );

            // Get size of permissions table
            const permissionsSizeResult = await this.executeQuery(
                `SELECT pg_relation_size($1) as size`,
                [permissionsTable],
            );

            // Sum the sizes
            const collectionSize =
                parseInt(collectionSizeResult.rows[0].size, 10) || 0;
            const permissionsSize =
                parseInt(permissionsSizeResult.rows[0].size, 10) || 0;

            return collectionSize + permissionsSize;
        } catch (e: any) {
            throw new DatabaseError(
                `Failed to get collection size: ${e.message}`,
            );
        }
    }

    async getSizeOfCollectionOnDisk(collection: string): Promise<number> {
        const filteredCollection = this.filter(collection);
        const tableName = this.getSQLTable(filteredCollection);
        const permissionsTable = this.getSQLTable(
            filteredCollection + "_perms",
        );

        try {
            // Get size of main collection table
            const collectionSizeResult = await this.executeQuery(
                `SELECT pg_total_relation_size($1) as size`,
                [tableName],
            );

            // Get size of permissions table
            const permissionsSizeResult = await this.executeQuery(
                `SELECT pg_total_relation_size($1) as size`,
                [permissionsTable],
            );

            // Sum the sizes
            const collectionSize =
                parseInt(collectionSizeResult.rows[0].size, 10) || 0;
            const permissionsSize =
                parseInt(permissionsSizeResult.rows[0].size, 10) || 0;

            return collectionSize + permissionsSize;
        } catch (e: any) {
            throw new DatabaseError(
                `Failed to get collection size: ${e.message}`,
            );
        }
    }

    getSupportForIndex(): boolean {
        return true;
    }

    getSupportForUniqueIndex(): boolean {
        return true;
    }

    getSupportForFulltextIndex(): boolean {
        return true;
    }

    getSupportForFulltextWildcardIndex(): boolean {
        return false;
    }

    getSupportForCasting(): boolean {
        return false;
    }

    getMinDateTime(): Date {
        return new Date("0001-01-01 00:00:00");
    }

    getSupportForTimeouts(): boolean {
        return true;
    }

    getSupportForJSONOverlaps(): boolean {
        return false;
    }

    getSupportForSchemaAttributes(): boolean {
        return false;
    }

    getSupportForUpserts(): boolean {
        return false;
    }

    getLikeOperator(): string {
        return "ILIKE";
    }

    protected getSQLTable(name: string): string {
        const prefixPart = this.prefix ? `${this.prefix}_` : "";
        return `"${this.getDatabase()}"."${prefixPart}${this.filter(name)}"`;
    }

    /**
     * Converts a query object to an SQL condition string.
     * @param query - The query object to convert
     * @returns A string containing the SQL condition
     */
    public getSQLCondition(query: Query): string {
        // Use a local parameter counter for thread safety
        let paramCount = 1;

        // Map special attributes to their database column names
        const attribute = query.getAttribute();
        const mappedAttribute = (() => {
            switch (attribute) {
                case "$id":
                    return "_uid";
                case "$internalId":
                    return "_id";
                case "$tenant":
                    return "_tenant";
                case "$createdAt":
                    return "_createdAt";
                case "$updatedAt":
                    return "_updatedAt";
                default:
                    return attribute;
            }
        })();

        // Set the mapped attribute back to the query
        query.setAttribute(mappedAttribute);

        const filteredAttr = this.filter(mappedAttribute);
        const quotedAttribute = `table_main."${filteredAttr}"`;
        const method = query.getMethod();

        switch (method) {
            case Query.TYPE_SEARCH:
                return `to_tsvector(regexp_replace(${quotedAttribute}, '[^\\w]+', ' ', 'g')) @@ websearch_to_tsquery(${this.getFulltextValue(query.getValue())})`;

            case Query.TYPE_BETWEEN:
                let betweenValues = query.getValues();
                return `${quotedAttribute} BETWEEN $${paramCount++} AND $${paramCount++}`;

            case Query.TYPE_IS_NULL:
            case Query.TYPE_IS_NOT_NULL:
                return `${quotedAttribute} ${this.getSQLOperator(method)}`;

            case Query.TYPE_EQUAL:
                return this.getSQLConditionEqual(
                    filteredAttr,
                    query.getValues(),
                );
            // @ts-ignore
            case Query.TYPE_CONTAINS:
                if (query.onArray()) {
                    return `${quotedAttribute} @> $${paramCount++}`;
                }
            // Fall through to default for non-array contains
            /* falls through */

            default:
                const conditions: string[] = [];
                let operator =
                    query.getMethod() === Query.TYPE_CONTAINS && query.onArray()
                        ? "@>"
                        : this.getSQLOperator(query.getMethod());

                let values = query.getValues();
                values.forEach((_, index) => {
                    conditions.push(
                        `${quotedAttribute} ${operator} $${paramCount++}`,
                    );
                });

                return conditions.length === 0
                    ? ""
                    : `(${conditions.join(" OR ")})`;
        }
    }

    /**
     * Generate condition for Query.equal operator
     * @param field - The field to check
     * @param values - The values to compare against
     * @returns SQL condition string
     * @throws Error if values format is incorrect
     */
    protected getSQLConditionEqual(field: string, values: any[]): string {
        // Use a local parameter counter for thread safety
        let paramCount = 1;

        if (!Array.isArray(values)) {
            throw new DatabaseError(
                "Invalid values format for equal condition",
            );
        }

        if (values.length === 0) {
            throw new DatabaseError("No values provided for equal condition");
        }

        if (values.length === 1) {
            // Special handling for boolean values
            if (typeof values[0] === "boolean") {
                return `"table_main"."${this.filter(field)}" = ${values[0] ? "TRUE" : "FALSE"}`;
            }

            // Handle null values
            if (values[0] === null) {
                return `"table_main"."${this.filter(field)}" IS NULL`;
            }

            return `"table_main"."${this.filter(field)}" = $${paramCount++}`;
        }

        const placeholders = [];
        for (let i = 0; i < values.length; i++) {
            // Special handling for boolean values
            if (typeof values[i] === "boolean") {
                placeholders.push(values[i] ? "TRUE" : "FALSE");
                // Remove the value from the array so it's not included in parameter binding
                values.splice(i, 1);
                i--; // Adjust index after removal
            } else if (values[i] === null) {
                placeholders.push("NULL");
                // Remove the value from the array so it's not included in parameter binding
                values.splice(i, 1);
                i--; // Adjust index after removal
            } else {
                placeholders.push(`$${paramCount++}`);
            }
        }

        return `"table_main"."${this.filter(field)}" IN (${placeholders.join(", ")})`;
    }

    /**
     * Binds the values of a query condition to the params array
     * @param params - The array to push parameters to
     * @param query - The query with values to bind
     */
    protected bindConditionValue(params: any[], query: Query): void {
        const method = query.getMethod();
        const values = query.getValues();

        if (values.length === 0) {
            return;
        }

        switch (method) {
            case Query.TYPE_BETWEEN:
                if (values.length >= 2) {
                    params.push(values[0], values[1]);
                }
                break;
            case Query.TYPE_IS_NULL:
            case Query.TYPE_IS_NOT_NULL:
                // No parameters needed for NULL checks
                break;
            case Query.TYPE_EQUAL:
                // Handle boolean and null values separately in getSQLConditionEqual
                for (const value of values) {
                    if (typeof value !== "boolean" && value !== null) {
                        params.push(value);
                    }
                }
                break;
            case Query.TYPE_CONTAINS:
                if (query.onArray()) {
                    // For array contains, wrap the value in an array
                    params.push(
                        Array.isArray(values[0])
                            ? JSON.stringify(values[0])
                            : JSON.stringify([values[0]]),
                    );
                } else {
                    // For regular contains, handle like other operators
                    for (const value of values) {
                        params.push(`%${value}%`);
                    }
                }
                break;
            default:
                for (const value of values) {
                    if (method === Query.TYPE_SEARCH) {
                        params.push(`%${value}%`);
                    } else {
                        params.push(value);
                    }
                }
                break;
        }
    }

    /**
     * Formats a string value for full-text search queries
     * @param value - The search string to format
     * @returns A formatted string suitable for full-text search
     */
    protected getFulltextValue(value: string): string {
        const exact = value.startsWith('"') && value.endsWith('"');

        // Remove special characters
        value = value.replace(/[@+\-*.'\"]/g, " ");

        // Remove multiple whitespaces
        value = value.replace(/\s+/g, " ").trim();

        if (!exact) {
            value = value.replace(/ /g, " or ");
        }

        return `'${value}'`;
    }

    /**
     * Implementation of trigger that uses EventEmitter from the parent class
     * @param event - Event name
     * @param data - Data to pass to event handlers (e.g., SQL query)
     * @returns The potentially modified data
     */
    protected async trigger<T>(event: string, data: T): Promise<T> {
        // First make a copy of the data to avoid unexpected side effects
        let result = data;

        // Get listeners for this event and the generic event
        const listeners = [
            ...this.listeners(event),
            ...this.listeners(Database.EVENT_ALL),
        ];

        // Emit events explicitly
        this.emit(event, data);
        this.emit(Database.EVENT_ALL, data);

        // Process any synchronous modifications listeners might have made
        // In the future, this could be enhanced to support async modifications
        return result;
    }

    /**
     * Set query timeout in milliseconds
     */
    public setTimeout(
        milliseconds: number,
        event: string = Database.EVENT_ALL,
    ): void {
        if (milliseconds < 0) {
            throw new DatabaseError(
                "Timeout value must be greater than or equal to 0",
            );
        }

        this.timeout = milliseconds;

        // Use the standard EventEmitter.emit method from parent class
        this.emit("timeout:set", { milliseconds, event });
    }

    processException(error: any): Error {
        // Check if the error is a PostgreSQL error
        if (error && error.code) {
            // Timeout
            if (error.code === "57014") {
                return new TimeoutException("Query timed out", error.code);
            }

            // Duplicate table
            if (error.code === "42P07") {
                return new DuplicateException(
                    "Collection already exists",
                    error.code,
                );
            }

            // Duplicate column
            if (error.code === "42701") {
                return new DuplicateException(
                    "Attribute already exists",
                    error.code,
                );
            }

            // Duplicate row
            if (error.code === "23505") {
                return new DuplicateException(
                    "Document already exists",
                    error.code,
                );
            }

            // Data is too big for column resize
            if (error.code === "22001") {
                return new TruncateException(
                    "Resize would result in data truncation",
                    error.code,
                );
            }
        }

        return error;
    }

    getSQLType(
        type: string,
        size: number,
        signed: boolean = true,
        array: boolean = false,
    ): string {
        if (array === true) {
            return "JSONB";
        }

        switch (type) {
            case Database.VAR_STRING:
                // Check if size exceeds maximum varchar length
                if (size > this.getMaxVarcharLength()) {
                    return "TEXT";
                }
                return `VARCHAR(${size})`;

            case Database.VAR_INTEGER:
                if (size >= 8) {
                    // INT = 4 bytes, BIGINT = 8 bytes
                    return "BIGINT";
                }
                return "INTEGER";

            case Database.VAR_FLOAT:
                return "DOUBLE PRECISION";

            case Database.VAR_BOOLEAN:
                return "BOOLEAN";

            case Database.VAR_RELATIONSHIP:
                return "VARCHAR(255)";

            case Database.VAR_DATETIME:
                return "TIMESTAMP(3)";

            default:
                throw new DatabaseError("Unknown Type: " + type);
        }
    }

    protected getAttributeProjection(
        selections: string[] | null,
        prefix: string = "",
    ): string {
        // If selections is empty or contains '*', return all columns
        if (
            !selections ||
            selections.length === 0 ||
            selections.includes("*")
        ) {
            if (prefix) {
                return `${prefix}.*`;
            }
            return "*";
        }

        // Clone the array to avoid modifying the original
        let selectionsCopy = [...selections];

        // Remove $id, $permissions and $collection from selections if present
        selectionsCopy = selectionsCopy.filter(
            (item) => !["$id", "$permissions", "$collection"].includes(item),
        );

        // Always include _uid and _permissions
        selectionsCopy.push("_uid");
        selectionsCopy.push("_permissions");

        // Handle special attributes
        if (selectionsCopy.includes("$internalId")) {
            selectionsCopy.push("_id");
            selectionsCopy = selectionsCopy.filter(
                (item) => item !== "$internalId",
            );
        }

        if (selectionsCopy.includes("$createdAt")) {
            selectionsCopy.push("_createdAt");
            selectionsCopy = selectionsCopy.filter(
                (item) => item !== "$createdAt",
            );
        }

        if (selectionsCopy.includes("$updatedAt")) {
            selectionsCopy.push("_updatedAt");
            selectionsCopy = selectionsCopy.filter(
                (item) => item !== "$updatedAt",
            );
        }

        // Apply prefix and quoting to each selection
        if (prefix) {
            return selectionsCopy
                .map((selection) => `"${prefix}"."${this.filter(selection)}"`)
                .join(", ");
        } else {
            return selectionsCopy
                .map((selection) => `"${this.filter(selection)}"`)
                .join(", ");
        }
    }

    /**
     * Generate SQL permission condition with parameter index
     *
     * @param collection - Collection name
     * @param roles - Authorization roles
     * @param forPermission - Permission type
     * @param paramIndex - Starting parameter index
     * @returns SQL condition for permissions
     */
    protected override getSQLPermissionsCondition(
        collection: string,
        roles: string[],
        forPermission: string = Database.PERMISSION_READ,
        paramIndex: number | any = 1,
    ): string {
        const permsTable = this.getSQLTable(collection + "_perms");

        let sql = `table_main._uid IN (
              SELECT _document
              FROM ${permsTable}
              WHERE _permission IN ('any'`;

        if (roles.length > 0) {
            sql += ", '" + roles.join("', '") + "'";
        }

        sql += `)
                AND _type = '${forPermission}'
                `;

        if (this.sharedTables) {
            sql += `AND _tenant = $${paramIndex}`;
        }

        sql += ")";

        return sql;
    }

    /**
     * Generates SQL conditions from an array of Query objects
     * This method handles parameter counting locally for thread safety
     *
     * @param queries - The array of Query objects
     * @returns A string containing the SQL conditions
     */
    public getSQLConditions(queries: Query[]): string {
        if (!queries || queries.length === 0) {
            return "";
        }

        const conditions: string[] = [];

        for (const query of queries) {
            const condition = this.getSQLCondition(query);
            if (condition) {
                conditions.push(condition);
            }
        }

        return conditions.length === 0 ? "" : `(${conditions.join(" AND ")})`;
    }

    /**
     * Creates SQL conditions for an array of queries with proper parameter indices
     * @param queries - The queries to create conditions for
     * @param startParamIndex - The starting parameter index
     * @returns Object containing conditions string and parameters array
     */
    protected getSQLConditionsWithParams(
        queries: Query[],
        startParamIndex: number = 1,
    ): { conditions: string; params: any[] } {
        const params: any[] = [];
        let currentParamIndex = startParamIndex;

        if (!queries || queries.length === 0) {
            return { conditions: "", params: [] };
        }

        const conditions: string[] = [];

        for (const query of queries) {
            // Generate SQL condition with the current parameter index
            const result = this.getSQLConditionWithParams(
                query,
                currentParamIndex,
            );
            if (result.condition) {
                conditions.push(result.condition);
                params.push(...result.params);
                currentParamIndex += result.params.length;
            }
        }

        return {
            conditions:
                conditions.length > 0 ? `(${conditions.join(" AND ")})` : "",
            params,
        };
    }

    /**
     * Converts a query object to an SQL condition string with parameter indices
     * @param query - The query object to convert
     * @param startParamIndex - The starting parameter index
     * @returns Object containing the SQL condition and parameters
     */
    protected getSQLConditionWithParams(
        query: Query,
        startParamIndex: number = 1,
    ): { condition: string; params: any[] } {
        const params: any[] = [];
        let currentParamIndex = startParamIndex;

        // Map special attributes to their database column names
        const attribute = query.getAttribute();
        const mappedAttribute = (() => {
            switch (attribute) {
                case "$id":
                    return "_uid";
                case "$internalId":
                    return "_id";
                case "$tenant":
                    return "_tenant";
                case "$createdAt":
                    return "_createdAt";
                case "$updatedAt":
                    return "_updatedAt";
                default:
                    return attribute;
            }
        })();

        // Set the mapped attribute back to the query
        query.setAttribute(mappedAttribute);

        const filteredAttr = this.filter(mappedAttribute);
        const quotedAttribute = `table_main."${filteredAttr}"`;
        const method = query.getMethod();
        const values = query.getValues();

        switch (method) {
            case Query.TYPE_SEARCH:
                params.push(this.getFulltextValue(query.getValue()));
                return {
                    condition: `to_tsvector(regexp_replace(${quotedAttribute}, '[^\\w]+', ' ', 'g')) @@ websearch_to_tsquery($${currentParamIndex})`,
                    params,
                };

            case Query.TYPE_BETWEEN:
                if (values.length >= 2) {
                    params.push(values[0], values[1]);
                    return {
                        condition: `${quotedAttribute} BETWEEN $${currentParamIndex} AND $${currentParamIndex + 1}`,
                        params,
                    };
                }
                return { condition: "", params: [] };

            case Query.TYPE_IS_NULL:
                return {
                    condition: `${quotedAttribute} IS NULL`,
                    params,
                };

            case Query.TYPE_IS_NOT_NULL:
                return {
                    condition: `${quotedAttribute} IS NOT NULL`,
                    params,
                };

            case Query.TYPE_EQUAL:
                return this.getSQLConditionEqualWithParams(
                    filteredAttr,
                    values,
                    currentParamIndex,
                );

            case Query.TYPE_CONTAINS:
                if (query.onArray()) {
                    params.push(
                        Array.isArray(values[0])
                            ? JSON.stringify(values[0])
                            : JSON.stringify([values[0]]),
                    );
                    return {
                        condition: `${quotedAttribute} @> $${currentParamIndex}`,
                        params,
                    };
                }
                // For string contains, use LIKE
                const containsConditions: string[] = [];
                for (let i = 0; i < values.length; i++) {
                    params.push(`%${values[i]}%`);
                    containsConditions.push(
                        `${quotedAttribute} LIKE $${currentParamIndex + i}`,
                    );
                }
                return {
                    condition:
                        containsConditions.length === 0
                            ? ""
                            : containsConditions.length === 1
                              ? containsConditions[0]
                              : `(${containsConditions.join(" OR ")})`,
                    params,
                };

            default:
                const conditions: string[] = [];
                const operator = this.getSQLOperator(query.getMethod());

                for (let i = 0; i < values.length; i++) {
                    params.push(values[i]);
                    conditions.push(
                        `${quotedAttribute} ${operator} $${currentParamIndex + i}`,
                    );
                }

                return {
                    condition:
                        conditions.length === 0
                            ? ""
                            : conditions.length === 1
                              ? conditions[0]
                              : `(${conditions.join(" OR ")})`,
                    params,
                };
        }
    }

    /**
     * Creates SQL condition for equality with parameters
     * @param field - The field name
     * @param values - The values to check equality against
     * @param startParamIndex - The starting parameter index
     * @returns SQL condition string and parameters
     */
    protected getSQLConditionEqualWithParams(
        field: string,
        values: any[],
        startParamIndex: number = 1,
    ): { condition: string; params: any[] } {
        if (!Array.isArray(values)) {
            throw new DatabaseError(
                "Invalid values format for equal condition",
            );
        }

        if (values.length === 0) {
            throw new DatabaseError("No values provided for equal condition");
        }

        const params: any[] = [];
        let currentParamIndex = startParamIndex;

        if (values.length === 1) {
            // Special handling for boolean values
            if (typeof values[0] === "boolean") {
                return {
                    condition: `"table_main"."${field}" = ${values[0] ? "TRUE" : "FALSE"}`,
                    params: [],
                };
            }

            // Handle null values
            if (values[0] === null) {
                return {
                    condition: `"table_main"."${field}" IS NULL`,
                    params: [],
                };
            }

            params.push(values[0]);
            return {
                condition: `"table_main"."${field}" = $${currentParamIndex}`,
                params,
            };
        }

        const placeholders: string[] = [];
        for (let i = 0; i < values.length; i++) {
            // Special handling for boolean values
            if (typeof values[i] === "boolean") {
                placeholders.push(values[i] ? "TRUE" : "FALSE");
            } else if (values[i] === null) {
                placeholders.push("NULL");
            } else {
                params.push(values[i]);
                placeholders.push(`$${currentParamIndex++}`);
            }
        }

        return {
            condition: `"table_main"."${field}" IN (${placeholders.join(", ")})`,
            params,
        };
    }

    /**
     * Generate SQL permission condition with parameter index
     *
     * @param collection - Collection name
     * @param roles - Authorization roles
     * @param forPermission - Permission type
     * @param paramIndex - Starting parameter index
     * @returns SQL condition for permissions
     */
    protected getSQLPermissionsConditionWithParam(
        collection: string,
        roles: string[],
        forPermission: string = Database.PERMISSION_READ,
        paramIndex: number = 1,
    ): string {
        const permsTable = this.getSQLTable(collection + "_perms");

        let sql = `table_main._uid IN (
              SELECT _document
              FROM ${permsTable}
              WHERE _permission IN ('any'`;

        if (roles.length > 0) {
            sql += ", '" + roles.join("', '") + "'";
        }

        sql += `)
                AND _type = '${forPermission}'
                `;

        if (this.sharedTables) {
            sql += `AND _tenant = $${paramIndex}`;
        }

        sql += ")";

        return sql;
    }

    /**
     * Centralized query execution with statistics tracking
     *
     * @param sql - SQL query to execute
     * @param params - Parameters for the query
     * @param client - Optional client for transaction support
     * @param description - Optional description for logging/stats
     * @returns Query result
     */
    protected async executeQuery<T = any>(
        sql: string,
        params: any[] = [],
        client?: any,
        description: string = "query",
    ): Promise<T> {
        const startTime = Date.now();
        const useClient = client || this.pool;
        let success = false;

        try {
            // Execute the query
            const result = await useClient.query(sql, params);

            // Update stats
            success = true;
            const executionTime = Date.now() - startTime;
            this.updateQueryStats(sql, executionTime, success, description);

            // If debugging is enabled, log slow queries
            if (this.debug && executionTime > 1000) {
                this.logger.warn(
                    `Slow query (${executionTime}ms): ${sql.substring(0, 100)}${sql.length > 100 ? "..." : ""}`,
                );
            }

            // Emit query execution event (can be used for monitoring/logging)
            this.emit("query:executed", {
                sql,
                params,
                executionTime,
                success: true,
                description,
                resultSize: result.rows?.length || 0,
            });

            return result;
        } catch (error: any) {
            // Update failure stats
            const executionTime = Date.now() - startTime;
            this.updateQueryStats(sql, executionTime, false, description);

            // Log the error
            this.logger.error(
                `Query error (${description}): ${error.message}`,
                {
                    sql: sql.substring(0, 200),
                    params: params,
                },
            );

            // Emit error event
            this.emit("query:error", {
                sql,
                params,
                executionTime,
                error,
                description,
            });

            // Process and re-throw the error
            throw this.processException(error);
        }
    }

    /**
     * Update query statistics
     */
    private updateQueryStats(
        sql: string,
        executionTime: number,
        success: boolean,
        description: string,
    ): void {
        // Update counters
        this.queryStats.totalQueries++;
        if (success) {
            this.queryStats.successfulQueries++;
        } else {
            this.queryStats.failedQueries++;
        }

        this.queryStats.totalTimeMs += executionTime;

        // Track slowest query
        if (executionTime > this.queryStats.slowestQueryMs) {
            this.queryStats.slowestQueryMs = executionTime;
            this.queryStats.slowestQuery = sql.substring(0, 200);
        }

        // Add to query log with rotation
        this.queryStats.queryLog.unshift({
            sql: sql.substring(0, 100),
            timeMs: executionTime,
            timestamp: Date.now(),
            success,
        });

        // Maintain maximum log size
        if (this.queryStats.queryLog.length > this.maxQueryLogSize) {
            this.queryStats.queryLog.pop();
        }

        // Calculate queries per second periodically
        const now = Date.now();
        if (now - this.queryStats.lastCalculated > this.statsInterval) {
            const timeWindowMs = now - this.queryStats.lastCalculated;
            const queriesInWindow = this.queryStats.queryLog.filter(
                (q) => q.timestamp > now - timeWindowMs,
            ).length;

            this.queryStats.queriesPerSecond =
                queriesInWindow / (timeWindowMs / 1000);
            this.queryStats.lastCalculated = now;

            // Emit stats update event
            this.emit("query:stats", { ...this.queryStats });
        }
    }

    /**
     * Get current query statistics
     */
    public getQueryStats() {
        return { ...this.queryStats };
    }

    /**
     * Reset query statistics
     */
    public resetQueryStats() {
        this.queryStats = {
            totalQueries: 0,
            successfulQueries: 0,
            failedQueries: 0,
            totalTimeMs: 0,
            slowestQueryMs: 0,
            slowestQuery: "",
            queriesPerSecond: 0,
            lastCalculated: Date.now(),
            queryLog: [],
        };
    }

    /**
     * Get comprehensive diagnostics about database connection and query performance
     * @returns Object containing detailed diagnostics
     */
    public async getDiagnostics(): Promise<any> {
        const now = Date.now();

        // Pool statistics
        const poolStats = this.getPoolStats();

        // Query statistics
        const queryStats = this.getQueryStats();

        // Calculate averages
        const avgQueryTime =
            queryStats.totalQueries > 0
                ? queryStats.totalTimeMs / queryStats.totalQueries
                : 0;

        // Recent query info
        const recentQueries = queryStats.queryLog.slice(0, 5);

        // Database version
        let dbVersion = "Unknown";
        try {
            const versionResult = await this.executeQuery(
                "SELECT version()",
                [],
                undefined,
                "diagnostics",
            );
            dbVersion = versionResult.rows?.[0]?.version || "Unknown";
        } catch (err) {
            this.logger.error("Error fetching database version:", err);
        }

        // Active connections
        let activeConnections = -1;
        try {
            const connectionsResult = await this.executeQuery(
                "SELECT count(*) as count FROM pg_stat_activity WHERE datname = current_database()",
                [],
                undefined,
                "diagnostics",
            );
            activeConnections =
                parseInt(connectionsResult.rows?.[0]?.count, 10) || -1;
        } catch (err) {
            this.logger.error("Error fetching active connections:", err);
        }

        // Return comprehensive diagnostics
        return {
            timestamp: now,
            poolStatus: {
                ...poolStats,
                maxConnections: this.defaultPoolConfig.max,
                connectionUtilization:
                    poolStats.totalConnections > 0
                        ? ((poolStats.totalConnections -
                              poolStats.idleConnections) /
                              poolStats.totalConnections) *
                          100
                        : 0,
            },
            queryPerformance: {
                ...queryStats,
                averageQueryTimeMs: avgQueryTime,
                errorRate:
                    queryStats.totalQueries > 0
                        ? (queryStats.failedQueries / queryStats.totalQueries) *
                          100
                        : 0,
                recentQueries,
            },
            databaseInfo: {
                version: dbVersion,
                activeConnections,
                uptime:
                    this.queryStats.lastCalculated > 0
                        ? now - this.queryStats.lastCalculated
                        : 0,
            },
            adapter: {
                type: this.type,
                database: this.database,
                schema: this.schema,
                inTransaction: this.inTransaction,
            },
        };
    }

    public getSupportForCastIndexArray(): boolean {
        return false;
    }
}
