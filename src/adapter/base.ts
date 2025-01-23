import { Constant as Database } from "../core/constant";
import { Document } from "../core/Document";
import { Logger } from "../core/logger";
import { Query } from "../core/query";
import { TransactionException } from "../errors";
import { DatabaseError } from "../errors/base";

export interface Adapter {
    ping(): Promise<void>;

    create(name: string): Promise<boolean>;

    getType(): string;

    getDatabase(): string;

    setDatabase(database: string): void;

    getSchema(): string;

    setSchema(schema: string): void;

    getSharedTables(): boolean;

    setSharedTables(sharedTables: boolean): void;

    getTenantId(): number | null;

    setTenantId(tenantId: number | null): void;

    getPrefix(): string;

    setPrefix(prefix: string): void;

    getDebug(): boolean;

    getInTransaction(): number;

    init(): Promise<void>;

    startTransaction(): Promise<boolean>;

    commitTransaction(): Promise<boolean>;

    rollbackTransaction(): Promise<boolean>;

    withTransaction<T>(callback: () => Promise<T>): Promise<T>;

    close(): Promise<void>;

    use(name: string): Promise<boolean>;

    create(name: string): Promise<boolean>;

    drop(name: string): Promise<boolean>;

    exists(name: string, collection?: string): Promise<boolean>;

    createCollection(
        name: string,
        attributes: Document[],
        indexes: Document[],
        ifExists?: boolean,
    ): Promise<boolean>;

    dropCollection(name: string, ifExists?: boolean): Promise<boolean>;

    createAttribute(
        collection: string,
        id: string,
        type: string,
        size: number,
        signed?: boolean,
        array?: boolean,
    ): Promise<boolean>;

    updateAttribute(
        collection: string,
        id: string,
        type: string,
        size: number,
        signed?: boolean,
        array?: boolean,
        newKey?: string,
    ): Promise<boolean>;

    deleteAttribute(collection: string, id: string): Promise<boolean>;

    renameAttribute(
        collection: string,
        oldName: string,
        newName: string,
    ): Promise<boolean>;

    createRelationship(
        collection: string,
        relatedCollection: string,
        type: string,
        twoWay?: boolean,
        id?: string,
        twoWayKey?: string,
    ): Promise<boolean>;

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
    ): Promise<boolean>;

    deleteRelationship(
        collection: string,
        relatedCollection: string,
        type: string,
        twoWay: boolean,
        key: string,
        twoWayKey: string,
        side: string,
    ): Promise<boolean>;

    renameIndex(
        collection: string,
        oldName: string,
        newName: string,
    ): Promise<boolean>;

    createIndex(
        collection: string,
        id: string,
        type: string,
        attributes: string[],
        lengths: number[],
        orders: string[],
    ): Promise<boolean>;

    deleteIndex(collection: string, id: string): Promise<boolean>;

    createDocument(collection: string, document: Document): Promise<Document>;

    createDocuments(
        collection: string,
        documents: Document[],
        batchSize?: number,
    ): Promise<Document[]>;

    updateDocument(
        collection: string,
        id: string,
        document: Document,
    ): Promise<Document>;

    updateDocuments(
        collection: string,
        updates: Document,
        documents: Document[],
    ): Promise<number>;

    increaseDocumentAttribute(
        collection: string,
        id: string,
        attribute: string,
        value: number,
        updatedAt: string,
        min?: number,
        max?: number,
    ): Promise<boolean>;

    deleteDocument(collection: string, uid: string): Promise<boolean>;

    deleteDocuments(collection: string, ids: string[]): Promise<number>;

    find(
        collection: string,
        queries?: Query[],
        limit?: number,
        offset?: number | null,
        orderAttributes?: string[],
        orderTypes?: string[],
        cursor?: any,
        cursorDirection?:
            | typeof Database.CURSOR_AFTER
            | typeof Database.CURSOR_BEFORE,
        forPermission?: string,
    ): Promise<Document[]>;

    count(
        collection: string,
        queries?: Query[],
        max?: number | null,
    ): Promise<number>;

    sum(
        collection: string,
        attribute: string,
        queries: Query[],
        max: number | null,
    ): Promise<number>;

    getDocument(
        collection: string,
        uid: string,
        queries?: Query[],
        forUpdate?: boolean,
    ): Promise<Document>;

    getDocuments(collection: string): Promise<Document[]>;

    before(event: string, name?: string, callback?: Function | null): this;

    getConnectionId(): string | number;

    getSizeOfCollection(collection: string): Promise<number>;

    getSizeOfCollectionOnDisk(collection: string): Promise<number>;

    getLimitForString(): number;

    getLimitForInt(): number;

    getLimitForAttributes(): number;

    getLimitForIndexes(): number;

    getMaxIndexLength(): number;

    getCountOfAttributes(collection: Document): number;

    getCountOfDefaultAttributes(): number;

    getCountOfDefaultIndexes(): number;

    getCountOfIndexes(collection: Document): number;

    getInternalIndexesKeys(): string[];

    getAttributeWidth(collection: Document): number;

    getDocumentSizeLimit(): number;

    getSupportForIndex(): boolean;

    getSupportForUniqueIndex(): boolean;

    getSupportForFulltextIndex(): boolean;

    getSupportForFulltextWildcardIndex(): boolean;

    filter(name: string): string;

    getSupportForCasting(): boolean;

    getMinDateTime(): Date;

    getMaxDateTime(): Date;

    isInitialized(): boolean;
}

interface IDatabaseAdapter {}

/**
 * Base adapter class
 */
export abstract class DatabaseAdapter implements IDatabaseAdapter {
    protected options: any;

    protected type: string;

    // @ts-ignore
    protected database: string;

    // @ts-ignore
    protected schema: string;

    protected sharedTables: boolean = false;

    protected tenantId: number | null = null;

    // @ts-ignore
    protected perfix: string;

    protected transformations: Record<string, Record<string, Function>> = {
        "*": {},
    };

    protected metadata: Record<string, any> = {};

    /**
     * Debug mode
     */
    protected debug: boolean = true;

    /**
     * Logger instance
     */
    protected logger: Logger;

    /**
     * Transaction counter
     */
    protected inTransaction: number = 0;

    constructor() {
        this.type = "base";
        this.logger = new Logger();
    }

    async withTransaction<T>(callback: () => Promise<T>): Promise<T> {
        for (let attempts = 0; attempts < 3; attempts++) {
            try {
                await this.startTransaction();
                const result = await callback();
                await this.commitTransaction();
                return result;
            } catch (action) {
                try {
                    await this.rollbackTransaction();
                } catch (rollback) {
                    if (attempts < 2) {
                        setTimeout(() => {}, 5);
                        continue;
                    }
                    this.inTransaction = 0;
                    throw rollback;
                }
                if (attempts < 2) {
                    setTimeout(() => {}, 5);
                    continue;
                }
                this.inTransaction = 0;
                throw action;
            }
        }
        throw new TransactionException("Failed to execute transaction");
    }

    /**
     * Get the type of the adapter
     */
    public getType(): string {
        return this.type;
    }

    /**
     * Get the database name
     */
    public getDatabase(): string {
        return this.database;
    }

    /**
     * Set the database name
     */
    public setDatabase(database: string): void {
        this.database = database;
        this.options.database = database;
        this.options.connection.database = database;
    }

    /**
     * Get the schema name
     */
    public getSchema(): string {
        return this.schema;
    }

    /**
     * Set the schema name
     */
    public setSchema(schema: string): void {
        this.schema = schema;
    }

    /**
     * Check if shared tables are enabled
     */
    public getSharedTables(): boolean {
        return this.sharedTables;
    }

    /**
     * Set shared tables
     */
    public setSharedTables(sharedTables: boolean): void {
        this.sharedTables = sharedTables;
    }

    /**
     * Get the tenant ID
     */
    public getTenantId(): number | null {
        return this.tenantId;
    }

    /**
     * Set the tenant ID
     */
    public setTenantId(tenantId: number): void {
        this.tenantId = tenantId;
    }

    /**
     * Get the prefix
     */
    public getPrefix(): string {
        return this.perfix;
    }

    /**
     * Set the prefix
     */
    public setPrefix(prefix: string): void {
        this.perfix = this.filter(prefix);
    }

    /**
     * Check if debug mode is enabled
     */
    public getDebug(): boolean {
        return this.debug;
    }

    /**
     * Get the transaction counter
     */
    public getInTransaction(): number {
        return this.inTransaction;
    }

    /**
     * Load module
     */
    protected loadModule(moduleName: string): any {
        return require(moduleName);
    }

    abstract init(): Promise<void>;

    abstract startTransaction(): Promise<boolean>;

    abstract commitTransaction(): Promise<boolean>;

    abstract rollbackTransaction(): Promise<boolean>;

    abstract close(): Promise<void>;

    filter(value: string): string {
        value = value.replace(/[^A-Za-z0-9_\-]/g, "");
        if (value === null) {
            throw new DatabaseError("Failed to filter key");
        }
        return value;
    }

    before(
        event: string,
        name: string = "",
        callback: Function | null = null,
    ): this {
        if (!this.transformations[event]) {
            this.transformations[event] = {};
        }
        if (callback === null) {
            delete this.transformations[event][name];
        } else {
            this.transformations[event][name] = callback;
        }
        return this;
    }

    protected async trigger<T extends any>(
        event: string,
        query: T,
    ): Promise<T> {
        this.logger.debug(`${event}: ${query}`);
        for (const callback of Object.values(
            this.transformations[Database.EVENT_ALL] || {},
        )) {
            query = await callback(query);
        }
        for (const callback of Object.values(
            this.transformations[event] || {},
        )) {
            query = await callback(query);
        }
        return query;
    }

    public getMaxDateTime(): Date {
        return new Date("9999-12-31T23:59:59Z");
    }
}
