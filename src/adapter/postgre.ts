import { Document } from "@/core/Document";
import { Query } from "@/core/query";
import { Adapter } from "./base";
import { Sql } from "./sql";
import { Pool, PoolClient, PoolConfig } from "pg";
import Transaction from "@/errors/Transaction";
import { Database } from "@/core/database";
import { DuplicateException } from "@/errors";

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
        throw new Error("Method not implemented.");
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
            throw new Error(`Failed to drop schema: ${e.message}`);
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

    createRelationship(
        collection: string,
        relatedCollection: string,
        type: string,
        twoWay?: boolean,
        id?: string,
        twoWayKey?: string,
    ): Promise<boolean> {
        throw new Error("Method not implemented.");
    }
    updateRelationship(
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
        throw new Error("Method not implemented.");
    }
    deleteRelationship(
        collection: string,
        relatedCollection: string,
        type: string,
        twoWay: boolean,
        key: string,
        twoWayKey: string,
        side: string,
    ): Promise<boolean> {
        throw new Error("Method not implemented.");
    }
    renameIndex(
        collection: string,
        oldName: string,
        newName: string,
    ): Promise<boolean> {
        throw new Error("Method not implemented.");
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
                throw new Error(
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

    deleteIndex(collection: string, id: string): Promise<boolean> {
        throw new Error("Method not implemented.");
    }
    createDocument(collection: string, document: Document): Promise<Document> {
        throw new Error("Method not implemented.");
    }
    createDocuments(
        collection: string,
        documents: Document[],
        batchSize?: number,
    ): Promise<Document[]> {
        throw new Error("Method not implemented.");
    }
    updateDocument(
        collection: string,
        id: string,
        document: Document,
    ): Promise<Document> {
        throw new Error("Method not implemented.");
    }
    updateDocuments(
        collection: string,
        updates: Document,
        documents: Document[],
    ): Promise<number> {
        throw new Error("Method not implemented.");
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
    getSizeOfCollection(collection: string): Promise<number> {
        throw new Error("Method not implemented.");
    }
    getSizeOfCollectionOnDisk(collection: string): Promise<number> {
        throw new Error("Method not implemented.");
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
