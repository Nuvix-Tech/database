import { Document } from "../core/Document";
import { Query } from "../core/query";
import { Adapter } from "./base";
import { Sql } from "./sql";
import { Pool, PoolConfig } from "pg";
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
     * @description PostgreSQL connection options
     */
    declare protected options: PostgreDBOptions;

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
            this.pool =
                this.options.connection instanceof Pool
                    ? this.options.connection
                    : new Pool(this.options.connection);
            this.instance = this;
        } catch (e) {
            this.logger.error(e);
            throw new InitializeError("MariaDB adapter initialization failed");
        }
    }

    ping(): Promise<void> {
        return new Promise((resolve, reject) => {
            this.pool.query("SELECT 1", (err) => {
                if (err) {
                    return reject(err);
                }
                resolve();
            });
        });
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
            await this.pool.query(triggeredSql);
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
     *  Get Tenant SQL
     */
    private getTenantSql() {
        return `AND (_tenant = $00000001 OR _tenant IS NULL)`;
    }

    public async getClient(): Promise<any> {
        if (this.pool) {
            const client = await this.pool.connect();
            return client;
        }
        throw new Error("No PostgreSQL connection pool available");
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
            await this.pool.query(triggeredSql);
            return true;
        } catch (e: any) {
            throw new DatabaseError(`Failed to drop schema: ${e.message}`);
        }
    }

    /**
     * Checks if a database schema exists
     * @param name - The name of the schema to check
     * @returns Promise resolving to true if schema exists, false otherwise
     */
    async exists(name: string, collection?: string): Promise<boolean> {
        const filteredName = this.filter(name);
        const sql = `SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = '${filteredName}')`;
        try {
            const res = await this.pool.query(sql);
            return res.rows[0].exists;
        } catch {
            return false;
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
        const prefix = this.getPrefix();
        const id = this.filter(name);
        const attributeStrings: string[] = [];
        let sqlStatements: string[] = [];

        for (const attribute of attributes) {
            const attrId = this.filter(
                attribute.getAttribute("key", attribute.getAttribute("$id")),
            );

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
                await this.pool.query(sql);
            }

            // Create indexes
            for (const index of indexes) {
                const indexId = this.filter(index.getId());
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
                    await this.pool.query(
                        `DROP TABLE IF EXISTS ${this.getSQLTable(id)}`,
                    );
                    await this.pool.query(
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
    async dropCollection(name: string, ifExists = false): Promise<boolean> {
        const id = this.filter(name);
        const ifExistsClause = ifExists ? "IF EXISTS" : "";

        let sql = `DROP TABLE ${ifExistsClause} ${this.getSQLTable(id)}, ${this.getSQLTable(id + "_perms")}`;
        sql = await this.trigger(Database.EVENT_COLLECTION_DELETE, sql);

        try {
            await this.pool.query(sql);
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
            await this.pool.query(sql);
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
                await this.pool.query(renameSql);

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
            await this.pool.query(alterSql);

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
            await this.pool.query(sql);
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
            await this.pool.query(sql);
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
            await this.pool.query(sql);
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
            await this.pool.query(sql);
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
            await this.pool.query(sql);
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
            await this.pool.query(sql);
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
            await this.pool.query(sql);
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
            await this.pool.query(sql);
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
            const result = await this.pool.query(sql, values);
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

                await this.pool.query(
                    triggeredPermissionsQuery,
                    permissionValues,
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
            const result = await this.pool.query(sql, values);

            // Update documents with their internal IDs
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

                await this.pool.query(triggeredPermSql, permissionValues);
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

        const result = await this.pool.query(sql, params);

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
        await this.pool.query(sql, params);
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
        await this.pool.query(sql, params);
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
            await this.pool.query(updateSql, updateParams);

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
            const result = await this.pool.query(sql, values);
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
        const attr = this.filter(attribute);

        let sql = `
            UPDATE ${this.getSQLTable(name)} 
            SET 
                "${attr}" = "${attr}" + $1,
                "_updatedAt" = $2
            WHERE _uid = $3
        `;

        // Prepare parameters
        const params: any[] = [value, updatedAt, id];
        let paramIndex = 4;

        // Add tenant condition for shared tables
        if (this.sharedTables) {
            sql += " AND _tenant = $" + paramIndex;
            params.push(this.getTenantId());
            paramIndex++;
        }

        // Add max/min constraints if provided
        if (max !== undefined) {
            sql += ` AND "${attr}" <= $${paramIndex}`;
            params.push(max);
            paramIndex++;
        }

        if (min !== undefined) {
            sql += ` AND "${attr}" >= $${paramIndex}`;
            params.push(min);
            paramIndex++;
        }

        // Apply trigger
        sql = await this.trigger(Database.EVENT_DOCUMENT_UPDATE, sql);

        try {
            // Execute the query
            const result = await this.pool.query(sql, params);
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
            const result = await this.pool.query(sql, params);
            const deleted = result.rowCount! > 0;

            // Execute permissions deletion regardless of whether document existed
            await this.pool.query(permsSql, permsParams);

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
            const result = await this.pool.query(sql, params);
            const deleted = result.rowCount || 0;

            // Execute permissions deletion regardless of whether documents existed
            await this.pool.query(
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
            if (i === 0 && cursor) {
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
        if (mappedOrderAttributes.length === 0 && cursor) {
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
            const conditions = this.getSQLConditions(queries);
            if (conditions) {
                where.push(conditions);
                let cParams: any = [];
                for (const query of queries) {
                    this.bindConditionValue(cParams, query);
                }
                params.push(...cParams);
                paramIndex *= cParams.length;
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
                this.getSQLPermissionsCondition(name, roles, forPermission),
            );
            if (this.sharedTables) {
                params.push(this.getTenantId());
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
            const result = await this.pool.query(sql, params);
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
                let cParams: any = [];
                for (const query of queries) {
                    this.bindConditionValue(cParams, query);
                }
                params.push(...cParams);
                paramIndex *= cParams.length;
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

            const result = await this.pool.query(sql, params);
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
                let cParams: any = [];
                for (const query of queries) {
                    this.bindConditionValue(cParams, query);
                }
                params.push(...cParams);
                paramIndex *= cParams.length;
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

            const result = await this.pool.query(sql, params);
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
            const result = await this.pool.query(sql, params);

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
            const result = await this.pool.query(
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
            const collectionSizeResult = await this.pool.query(
                `SELECT pg_relation_size($1) as size`,
                [tableName],
            );

            // Get size of permissions table
            const permissionsSizeResult = await this.pool.query(
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
            const collectionSizeResult = await this.pool.query(
                `SELECT pg_total_relation_size($1) as size`,
                [tableName],
            );

            // Get size of permissions table
            const permissionsSizeResult = await this.pool.query(
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
        return true;
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

        const quotedAttribute = `"${this.filter(mappedAttribute)}"`;
        const method = query.getMethod();

        switch (method) {
            case Query.TYPE_SEARCH:
                return `to_tsvector(regexp_replace(${quotedAttribute}, '[^\\w]+', ' ', 'g')) @@ websearch_to_tsquery(${this.getFulltextValue(query.getValue())})`;

            case Query.TYPE_BETWEEN:
                let _values = query.getValues();
                return `table_main.${quotedAttribute} BETWEEN $1 AND $2`;

            case Query.TYPE_IS_NULL:
            case Query.TYPE_IS_NOT_NULL:
                return `table_main.${quotedAttribute} ${this.getSQLOperator(method)}`;
            // @ts-ignore
            case Query.TYPE_CONTAINS:
                let _operator = query.onArray()
                    ? "@>"
                    : this.getSQLOperator(method);

            // Fall through to default case for condition building

            default:
                const conditions: string[] = [];
                let operator =
                    query.getMethod() === Query.TYPE_CONTAINS && query.onArray()
                        ? "@>"
                        : this.getSQLOperator(query.getMethod());

                let values = query.getValues();
                values.forEach((_, index) => {
                    conditions.push(
                        `${quotedAttribute} ${operator} $${index + 1}`,
                    );
                });

                return conditions.length === 0
                    ? ""
                    : `(${conditions.join(" OR ")})`;
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

    public setTimeout(
        milliseconds: number,
        event: string = Database.EVENT_ALL,
    ): void {
        if (!this.getSupportForTimeouts()) {
            return;
        }

        if (milliseconds <= 0) {
            throw new DatabaseError("Timeout must be greater than 0");
        }

        this.timeout = milliseconds;

        this.before(event, "timeout", (sql: string) => {
            return `
                SET statement_timeout = ${milliseconds};
                ${sql};
                SET statement_timeout = 0;
            `;
        });
    }

    async close(): Promise<void> {
        if (this.pool) {
            await this.pool.end();
        }
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

    protected override getSQLPermissionsCondition(
        collection: string,
        roles: string[],
        type: string = Database.PERMISSION_READ,
    ): string {
        if (!Database.PERMISSIONS.includes(type)) {
            throw new DatabaseError("Unknown permission type: " + type);
        }
        const quotedRoles = roles.map((r) => `'${r}'`).join(", ");
        let tenantQuery = "";
        if (this.sharedTables) {
            tenantQuery = `AND (_tenant = $0001} OR _tenant IS NULL)`;
        }
        return `table_main._uid IN (
          SELECT _document
          FROM ${this.getSQLTable(collection + "_perms")}
          WHERE _permission IN (${quotedRoles})
            AND _type = '${type}'
            ${tenantQuery}
        )`;
    }
}
