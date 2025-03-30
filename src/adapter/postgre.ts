import { Document } from "@/core/Document";
import { Query } from "@/core/query";
import { Adapter } from "./base";
import { Sql } from "./sql";
import { Pool, PoolClient, PoolConfig } from "pg";
import Transaction from "@/errors/Transaction";
import { Database } from "@/core/database";
import { DatabaseError, DuplicateException } from "@/errors";

interface PostgreDBOptions {
    connection: PoolConfig | Pool;
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

    /**
     * @description PostgreSQL connection options
     */
    declare protected options: PostgreDBOptions;

    constructor(options: PostgreDBOptions) {
        super();
        this.options = options;
        this.type = "postgresql";
        this.pool =
            this.options.connection instanceof Pool
                ? this.options.connection
                : new Pool(this.options.connection);
    }

    isInitialized(): boolean {
        return this.instance !== null;
    }

    // TODO: Implement this method
    async init(): Promise<void> {
        if (this.instance) {
            return;
        }
        this.instance = this;
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
    async startTransaction(): Promise<boolean> {
        try {
            let client: PoolClient;

            if (this.inTransaction === 0) {
                // Get a client from the pool
                client = await this.pool.connect();

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
    async commitTransaction(): Promise<boolean> {
        if (this.inTransaction === 0) {
            return false;
        }

        try {
            this.inTransaction--;

            if (this.inTransaction === 0) {
                await this.pool.query("COMMIT");
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
    async rollbackTransaction(): Promise<boolean> {
        if (this.inTransaction === 0) {
            return false;
        }

        try {
            await this.pool.query("ROLLBACK");
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
        const sqlStatements: string[] = [];

        for (const attribute of attributes) {
            const attrId = this.filter(attribute.getId());

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
        ${attributeStrings.join(", ")}
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

        // Apply triggers to SQL statements
        for (let i = 0; i < sqlStatements.length; i++) {
            sqlStatements[i] = await this.trigger(
                Database.EVENT_COLLECTION_CREATE,
                sqlStatements[i],
            );
        }

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

        if (this.sharedTables) {
            attributes["_tenant"] = document.getAttribute(
                "_tenant",
                document.getAttribute("$tenant"),
            );
        }

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

        // Add all attributes
        for (const [attribute, value] of Object.entries(attributes)) {
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
                Object.keys(document.getAttributes()).forEach((key) =>
                    attributeKeys.add(key),
                );

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
            attributes["_createdAt"] = document.getCreatedAt();
            attributes["_updatedAt"] = document.getUpdatedAt();
            attributes["_permissions"] = JSON.stringify(
                document.getPermissions(),
            );

            if (this.sharedTables) {
                attributes["_tenant"] = tenantId;
            }

            const setClauses: string[] = [];
            const updateParams: any[] = [document.getId(), id]; // [new uid, existing uid]
            let paramIndex = 3;

            if (this.sharedTables) {
                updateParams.push(tenantId);
                paramIndex++;
            }

            for (const [attr, value] of Object.entries(attributes)) {
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

    increaseDocumentAttribute(
        collection: string,
        id: string,
        attribute: string,
        value: number,
        updatedAt: string,
        min?: number,
        max?: number,
    ): Promise<boolean> {
        throw new Error("Method not implemented.");
    }
    deleteDocument(collection: string, uid: string): Promise<boolean> {
        throw new Error("Method not implemented.");
    }
    deleteDocuments(collection: string, ids: string[]): Promise<number> {
        throw new Error("Method not implemented.");
    }
    find(
        collection: string,
        queries?: Query[],
        limit?: number,
        offset?: number | null,
        orderAttributes?: string[],
        orderTypes?: string[],
        cursor?: any,
        cursorDirection?: "after" | "before",
        forPermission?: string,
    ): Promise<Document[]> {
        throw new Error("Method not implemented.");
    }
    count(
        collection: string,
        queries?: Query[],
        max?: number | null,
    ): Promise<number> {
        throw new Error("Method not implemented.");
    }
    sum(
        collection: string,
        attribute: string,
        queries: Query[],
        max: number | null,
    ): Promise<number> {
        throw new Error("Method not implemented.");
    }
    getDocument(
        collection: string,
        uid: string,
        queries?: Query[],
        forUpdate?: boolean,
    ): Promise<Document> {
        throw new Error("Method not implemented.");
    }
    getDocuments(collection: string): Promise<Document[]> {
        throw new Error("Method not implemented.");
    }
    getConnectionId(): string | number {
        throw new Error("Method not implemented.");
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
        throw new Error("Method not implemented.");
    }
    getSupportForUniqueIndex(): boolean {
        throw new Error("Method not implemented.");
    }
    getSupportForFulltextIndex(): boolean {
        throw new Error("Method not implemented.");
    }
    getSupportForFulltextWildcardIndex(): boolean {
        throw new Error("Method not implemented.");
    }
    getSupportForCasting(): boolean {
        throw new Error("Method not implemented.");
    }
    getMinDateTime(): Date {
        throw new Error("Method not implemented.");
    }

    public getSQLCondition(query: Query): string {
        throw new Error("Method not implemented.");
    }
    public setTimeout(milliseconds: number, event: string): void {
        throw new Error("Method not implemented.");
    }

    close(): Promise<void> {
        throw new Error("Method not implemented.");
    }

    processException(error: any): Error {
        return error;
    }

    getSQLType(
        type: string,
        size: number,
        signed: boolean,
        array: boolean,
    ): string {
        if (array) {
            return `${type.toUpperCase()}[]`;
        }

        switch (type) {
            case "string":
                return `VARCHAR(${size})`;
            case "integer":
                return signed ? "INTEGER" : "BIGINT";
            case "float":
                return signed ? "FLOAT" : "DOUBLE PRECISION";
            case "boolean":
                return "BOOLEAN";
            case "date":
                return "TIMESTAMP(3)";
            default:
                return type.toUpperCase();
        }
    }
    getSQLTable(name: string): string {
        const prefix = this.getPrefix();
        return `"${prefix}_${name}"`;
    }
}
