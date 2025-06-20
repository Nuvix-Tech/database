import type { Adapter } from "../adapter/base";
import { Document } from "./Document";
import {
    AuthorizationException,
    ConflictException,
    DuplicateException,
    LimitException,
    NotFoundException,
    QueryException,
    RelationshipException,
    RestrictedException,
    StructureException,
} from "../errors";
import { ID } from "./ID";
import { Query } from "./query";
import Role from "../security/Role";
import type { Filter } from "./types/filter";
import crypto from "crypto";
import { IndexValidator } from "./validator";
import { DatabaseError } from "../errors/base";
import Permission from "../security/Permission";
import { Permissions } from "../security/Permissions";
import { Cache } from "@nuvix/cache";
import { Logger, type LoggerOptions } from "./logger";
import { Constant } from "./constant";
import { DateTime } from "./date-time";
import { Repository } from "./repository";
import { Structure } from "./validator/Structure";
import { Authorization } from "../security/authorization";
import { PartialStructure } from "./validator/PartialStructure";
import { DatabaseError as DatabaseException } from "../errors/base";
import { Document as DocumentValidator } from "./validator/Queries/Document";
import { Documents as DocumentsValidator } from "./validator/Queries/Documents";
import type {
    CreateAttributeParams,
    CreateCollectionParams,
    CreateIndexParams,
    CreateRelationshipParams,
    DecreaseDocumentAttributeParams,
    FindParams,
    GetDocumentParams,
    IncreaseDocumentAttributeParams,
    UpdateAttributeParams,
    UpdateCollectionParams,
    UpdateRelationshipParams,
} from "./types/database";

interface DatabaseOptions {
    cache?: any;
    filters?: Record<string, Filter>;
    entities?: any[];
    logger?: boolean | LoggerOptions;
    cacheTTL?: number;
    cacheEnabled?: boolean;
}

type IRecord = Record<string, unknown | any>;

export class Database extends Constant {
    protected adapter: Adapter;

    protected options: any;

    protected cache: Cache;

    protected cacheName: string;

    protected repositories: Map<any, Repository<any>> = new Map();

    protected entities: any[] = [];

    protected map: Record<string, boolean | string> = {};

    protected static filters: Record<string, Filter> = {};

    protected instanceFilters: Record<string, Filter> = {};

    protected listeners: Record<
        string,
        Record<
            string,
            | ((event: string, value: any) => any)
            | ((event: string, value: any) => Promise<any>)
        >
    > = {
        "*": {},
    };

    protected silentListeners: Record<string, boolean> | null = {};

    protected timestamp: Date | null = null;

    protected resolveRelationships: boolean = true;

    protected checkRelationshipsExist: boolean = true;

    protected relationshipFetchDepth: number = 1;

    protected filter: boolean = true;

    protected validate: boolean = true;

    protected preserveDates: boolean = false;

    protected maxQueryValues: number = 100;

    protected migrating: boolean = false;

    protected relationshipWriteStack: string[] = [];

    protected relationshipFetchStack: Document[] = [];

    protected relationshipDeleteStack: Document[] = [];

    protected globalCollections: Record<string, boolean> = {};

    protected logger: Logger;

    protected cacheTTL: number = 60 * 60 * 24; // 24 hours
    protected cacheEnabled: boolean = true;

    constructor(
        adapter: Adapter,
        cache: Cache,
        options: DatabaseOptions = {
            filters: {},
            entities: [],
        },
    ) {
        super();
        if (!adapter.isInitialized())
            throw new DatabaseError(
                "Adapter Should Initialize before passing to Database.",
            );
        this.adapter = adapter;
        this.cache = cache;
        this.instanceFilters = options.filters ?? {};
        this.cacheName = "default";
        this.logger = new Logger(options.logger);
        this.cacheTTL = options.cacheTTL ?? 60 * 60 * 24; // Default to 24 hours
        this.cacheEnabled = options.cacheEnabled ?? true;

        this.entities = options.entities as any[];

        Database.addFilter("json", {
            encode: (value: any, ...args) => {
                if (typeof value === "string") {
                    return value;
                }

                if (Array.isArray(value)) {
                    value = value.map((item) =>
                        item instanceof Document ? item.toObject() : item,
                    );
                } else if (value instanceof Document) {
                    value = value.toObject();
                }

                return JSON.stringify(value);
            },
            decode: (value: any, ...args) => {
                if (typeof value !== "string") {
                    return value;
                }

                try {
                    const parsed = JSON.parse(value);

                    if (!parsed) {
                        return parsed;
                    }

                    if (Array.isArray(parsed)) {
                        return parsed.map((item) => {
                            if (
                                item &&
                                typeof item === "object" &&
                                "$id" in item
                            ) {
                                return new Document(item);
                            }
                            return item;
                        });
                    }

                    if (
                        parsed &&
                        typeof parsed === "object" &&
                        "$id" in parsed
                    ) {
                        return new Document(parsed);
                    }

                    return parsed;
                } catch (e) {
                    this.logger.error("JSON FILTER ERROR:", e);
                    return value;
                }
            },
        });

        Database.addFilter("datetime", {
            encode: (value: string | Date | number): string | null => {
                if (!value) return null;

                try {
                    const date = new Date(value);
                    if (isNaN(date.getTime())) {
                        throw new DatabaseError(`Invalid date input: ${value}`);
                    }

                    return DateTime.format(date, DateTime.FORMAT_DB);
                } catch (error) {
                    this.logger.error("Failed to encode datetime:", {
                        value,
                        error,
                    });
                    return null; // Consistent return type
                }
            },

            decode: (value: string | null): string | null => {
                if (!value) return null;

                try {
                    return DateTime.formatTz(value);
                } catch (error) {
                    this.logger.error("Failed to decode datetime:", {
                        value,
                        error,
                    });
                    return null;
                }
            },
        });
    }

    /**
     * Add listener to events
     *
     * @param event
     * @param name
     * @param callback
     * @return this
     */
    public on(
        event: string,
        name: string,
        callback:
            | ((event: string, value: any) => any)
            | ((event: string, value: any) => Promise<any>),
    ): this {
        if (!this.listeners[event]) {
            this.listeners[event] = {};
        }
        this.listeners[event][name] = callback;

        return this;
    }

    /**
     * Add a transformation to be applied to a query string before an event occurs
     *
     * @param event
     * @param name
     * @param callback
     * @return this
     */
    public before(
        event: string,
        name: string,
        callback: ((value: any) => any) | ((value: any) => Promise<any>),
    ): this {
        this.adapter.before(event, name, callback);

        return this;
    }

    /**
     * Silent event generation for calls inside the callback
     *
     * @template T
     * @param callback
     * @param listeners List of listeners to silence; if null, all listeners will be silenced
     * @return T
     */
    public async silent<T>(
        callback: () => Promise<T>,
        listeners: string[] | null = null,
    ): Promise<T> {
        const previous = this.silentListeners;

        if (listeners === null) {
            this.silentListeners = null;
        } else {
            const silentListeners: Record<string, boolean> = {};
            for (const listener of listeners) {
                silentListeners[listener] = true;
            }
            this.silentListeners = silentListeners;
        }

        try {
            return await callback();
        } finally {
            this.silentListeners = previous;
        }
    }

    /**
     * Get getConnection Id
     *
     * @return string
     * @throws Error
     */
    public getConnectionId(): Promise<string | number> {
        return this.adapter.getConnectionId();
    }

    /**
     * Skip relationships for all the calls inside the callback
     *
     * @template T
     * @param callback
     * @return T
     */
    public async skipRelationships<T>(callback: () => Promise<T>): Promise<T> {
        const previous = this.resolveRelationships;
        this.resolveRelationships = false;

        try {
            return await callback();
        } finally {
            this.resolveRelationships = previous;
        }
    }

    public async skipRelationshipsExistCheck<T>(
        callback: () => Promise<T>,
    ): Promise<T> {
        const previous = this.checkRelationshipsExist;
        this.checkRelationshipsExist = false;

        try {
            return await callback();
        } finally {
            this.checkRelationshipsExist = previous;
        }
    }

    /**
     * Trigger callback for events
     *
     * @param event
     * @param args
     * @return void
     */
    private async trigger(event: string, args: any = null): Promise<void> {
        if (this.silentListeners === null) {
            return;
        }
        for (const [name, callback] of Object.entries(
            this.listeners[Database.EVENT_ALL] || {},
        )) {
            if (this.silentListeners[name]) {
                continue;
            }
            await callback(event, args);
        }

        for (const [name, callback] of Object.entries(
            this.listeners[event] || {},
        )) {
            if (this.silentListeners[name]) {
                continue;
            }
            await callback(event, args);
        }
    }

    /**
     * Executes callback with timestamp set to requestTimestamp
     *
     * @template T
     * @param requestTimestamp
     * @param callback
     * @return T
     */
    public async withRequestTimestamp<T>(
        requestTimestamp: Date | null,
        callback: () => Promise<T>,
    ): Promise<T> {
        const previous = this.timestamp;
        this.timestamp = requestTimestamp;
        try {
            return await callback();
        } finally {
            this.timestamp = previous;
        }
    }

    /**
     * Set Prefix.
     *
     * Set prefix to divide different scope of data sets
     *
     * @param prefix
     * @return this
     */
    public setPrefix(prefix: string): this {
        this.adapter.setPrefix(prefix);
        return this;
    }

    /**
     * Get Prefix.
     *
     * Get prefix of current set scope
     *
     * @return string
     */
    public getPrefix(): string {
        return this.adapter.getPrefix();
    }

    /**
     * Set database to use for current scope
     *
     * @param name
     * @return this
     */
    public setDatabase(name: string): this {
        this.adapter.setDatabase(name);
        return this;
    }

    /**
     * Get Database.
     *
     * Get Database from current scope
     *
     * @return string
     */
    public getDatabase(): string {
        return this.adapter.getDatabase();
    }

    /**
     * Set the cache instance
     *
     * @param cache
     * @return this
     */
    public setCache(cache: Cache): this {
        this.cache = cache;
        return this;
    }

    /**
     * Get the cache instance
     *
     * @return Cache
     */
    public getCache(): Cache {
        return this.cache;
    }

    /**
     * Set the name to use for cache
     *
     * @param name
     * @return this
     */
    public setCacheName(name: string): this {
        this.cacheName = name;
        return this;
    }

    /**
     * Get the cache name
     *
     * @return string
     */
    public getCacheName(): string {
        return this.cacheName;
    }

    /**
     * Enable filters
     *
     * @return this
     */
    public enableFilters(): this {
        this.filter = true;
        return this;
    }

    /**
     * Disable filters
     *
     * @return this
     */
    public disableFilters(): this {
        this.filter = false;
        return this;
    }

    /**
     * Skip filters
     *
     * Execute a callback without filters
     *
     * @template T
     * @param callback
     * @return T
     */
    public async skipFilters<T>(callback: () => Promise<T>): Promise<T> {
        const initial = this.filter;
        this.disableFilters();

        try {
            return await callback();
        } finally {
            this.filter = initial;
        }
    }

    /**
     * Get instance filters
     *
     * @return Record<string, Filter>
     */
    public getInstanceFilters(): Record<string, Filter> {
        return this.instanceFilters;
    }

    /**
     * Enable validation
     *
     * @return this
     */
    public enableValidation(): this {
        this.validate = true;
        return this;
    }

    /**
     * Disable validation
     *
     * @return this
     */
    public disableValidation(): this {
        this.validate = false;
        return this;
    }

    /**
     * Skip Validation
     *
     * Execute a callback without validation
     *
     * @template T
     * @param callback
     * @return T
     */
    public async skipValidation<T>(callback: () => Promise<T>): Promise<T> {
        const initial = this.validate;
        this.disableValidation();

        try {
            return await callback();
        } finally {
            this.validate = initial;
        }
    }

    /**
     * Get shared tables
     *
     * Get whether to share tables between tenants
     * @return boolean
     */
    public getSharedTables(): boolean {
        return this.adapter.getSharedTables();
    }

    /**
     * Set shared tables
     *
     * Set whether to share tables between tenants
     *
     * @param sharedTables
     * @return this
     */
    public setSharedTables(sharedTables: boolean): this {
        this.adapter.setSharedTables(sharedTables);
        return this;
    }

    /**
     * Set Tenant
     *
     * Set tenant to use if tables are shared
     *
     * @param tenant
     * @return this
     */
    public setTenant(tenant: number | null): this {
        this.adapter.setTenantId(tenant);
        return this;
    }

    /**
     * Get Tenant
     *
     * Get tenant to use if tables are shared
     *
     * @return number | null
     */
    public getTenant(): number | null {
        return this.adapter.getTenantId();
    }

    /**
     * With Tenant
     *
     * Execute a callback with a specific tenant
     *
     * @param tenant
     * @param callback
     * @return T
     */
    public async withTenant<T>(
        tenant: number | null,
        callback: () => Promise<T>,
    ): Promise<T> {
        const previous = this.adapter.getTenantId();
        this.adapter.setTenantId(tenant);

        try {
            return await callback();
        } finally {
            this.adapter.setTenantId(previous);
        }
    }

    public getPreserveDates(): boolean {
        return this.preserveDates;
    }

    public setPreserveDates(preserve: boolean): this {
        this.preserveDates = preserve;
        return this;
    }

    public setMigrating(migrating: boolean): this {
        this.migrating = migrating;
        return this;
    }

    public isMigrating(): boolean {
        return this.migrating;
    }

    public async withPreserveDates<T>(callback: () => Promise<T>): Promise<T> {
        const previous = this.preserveDates;
        this.preserveDates = true;

        try {
            return await callback();
        } finally {
            this.preserveDates = previous;
        }
    }

    public setMaxQueryValues(max: number): this {
        this.maxQueryValues = max;
        return this;
    }

    public getMaxQueryValues(): number {
        return this.maxQueryValues;
    }

    /**
     * Get Database Adapter
     *
     * @return Adapter
     */
    public getAdapter(): Adapter {
        return this.adapter;
    }

    public async close(): Promise<void> {
        return await this.adapter.close();
    }

    /**
     * Start a new transaction.
     *
     * If a transaction is already active, this will only increment the transaction count and return true.
     *
     * @return boolean
     * @throws DatabaseException
     */
    public async startTransaction(c: any): Promise<boolean> {
        return await this.adapter.startTransaction(c);
    }

    /**
     * Commit a transaction.
     *
     * If no transaction is active, this will be a no-op and will return false.
     * If there is more than one active transaction, this decrement the transaction count and return true.
     * If the transaction count is 1, it will be committed, the transaction count will be reset to 0, and return true.
     *
     * @return boolean
     * @throws DatabaseException
     */
    public async commitTransaction(c: any): Promise<boolean> {
        return await this.adapter.commitTransaction(c);
    }

    /**
     * Rollback a transaction.
     *
     * If no transaction is active, this will be a no-op and will return false.
     * If 1 or more transactions are active, this will roll back all transactions, reset the count to 0, and return true.
     *
     * @return boolean
     * @throws DatabaseException
     */
    public async rollbackTransaction(c: any): Promise<boolean> {
        return await this.adapter.rollbackTransaction(c);
    }

    /**
     * Execute a callback within a transaction
     *
     * @template T
     * @param callback
     * @return Promise<T>
     * @throws Error
     */
    public async withTransaction<T>(callback: () => Promise<T>): Promise<T> {
        return await this.adapter.withTransaction(callback);
    }

    /**
     * Ping Database
     *
     * @return Promise<boolean>
     * @throws Error
     */
    public async ping(): Promise<boolean> {
        await this.adapter.ping();
        return true;
    }

    /**
     * Create the database
     *
     * @param database
     * @return Promise<boolean>
     * @throws DuplicateException
     * @throws LimitException
     * @throws Error
     */
    public async create(database?: string): Promise<boolean> {
        database = database ?? this.adapter.getDatabase();

        await this.adapter.create(database);

        const attributes: Document<any>[] = Database.COLLECTION.attributes.map(
            (att) => new Document(att),
        );

        await this.silent(
            async () =>
                await this.createCollection(Database.METADATA, attributes),
        );

        await this.trigger(Database.EVENT_DATABASE_CREATE, database);

        return true;
    }

    /**
     * Check if database exists
     * Optionally check if collection exists in database
     *
     * @param database (optional) database name
     * @param collection (optional) collection name
     *
     * @return Promise<boolean>
     */
    public async exists(
        database?: string,
        collection?: string,
    ): Promise<boolean> {
        database = database ?? this.adapter.getDatabase();

        return await this.adapter.exists(database, collection);
    }

    /**
     * Delete Database
     *
     * @param database
     * @return Promise<boolean>
     * @throws DatabaseException
     */
    public async delete(database?: string): Promise<boolean> {
        database = database ?? this.adapter.getDatabase();

        const deleted = await this.adapter.drop(database);

        await this.trigger(Database.EVENT_DATABASE_DELETE, {
            name: database,
            deleted: deleted,
        });

        await this.cache.flush();

        return deleted;
    }

    /**
     * Create Collection
     *
     * @return Promise<Document>
     * @throws DatabaseException
     * @throws DuplicateException
     * @throws LimitException
     */
    public async createCollection<T extends IRecord>(
        params: CreateCollectionParams,
    ): Promise<Document<T>>;
    public async createCollection<T extends IRecord>(
        id: string,
        attributes?: Document[],
        indexes?: Document[],
        permissions?: string[] | null,
        documentSecurity?: boolean,
    ): Promise<Document<T>>;
    public async createCollection<T extends IRecord>(
        idOrParams: string | CreateCollectionParams,
        attributes: Document[] = [],
        indexes: Document[] = [],
        permissions: string[] | null = null,
        documentSecurity: boolean = true,
    ): Promise<Document<T>> {
        let id: string;

        if (typeof idOrParams === "string") {
            id = idOrParams;
        } else {
            id = idOrParams.id;
            attributes = idOrParams.attributes ?? [];
            indexes = idOrParams.indexes ?? [];
            permissions = idOrParams.permissions ?? null;
            documentSecurity = idOrParams.documentSecurity ?? true;
        }

        permissions ??= [Permission.create(Role.any())];

        if (this.validate) {
            const validator = new Permissions();
            if (!validator.isValid(permissions)) {
                throw new DatabaseError(validator.getDescription());
            }
        }

        let collection = await this.silent(
            async () => await this.getCollection(id),
        );

        if (!collection.isEmpty() && id !== Database.METADATA) {
            throw new DuplicateException(`Collection ${id} already exists`);
        }

        collection = new Document<any>({
            $id: ID.custom(id),
            $permissions: permissions,
            name: id,
            attributes: attributes,
            indexes: indexes,
            documentSecurity: documentSecurity,
        });

        if (this.validate) {
            const validator = new IndexValidator(
                attributes,
                this.adapter.getMaxIndexLength(),
                this.adapter.getInternalIndexesKeys(),
            );
            for (const index of indexes) {
                if (!validator.isValid(index)) {
                    throw new DatabaseError(validator.getDescription());
                }
            }
        }

        if (
            indexes &&
            this.adapter.getCountOfIndexes(collection) >
                this.adapter.getLimitForIndexes()
        ) {
            throw new LimitException(
                `Index limit of ${this.adapter.getLimitForIndexes()} exceeded. Cannot create collection.`,
            );
        }

        if (attributes) {
            if (
                this.adapter.getLimitForAttributes() > 0 &&
                this.adapter.getCountOfAttributes(collection) >
                    this.adapter.getLimitForAttributes()
            ) {
                throw new LimitException(
                    `Attribute limit of ${this.adapter.getLimitForAttributes()} exceeded. Cannot create collection.`,
                );
            }
            if (
                this.adapter.getDocumentSizeLimit() > 0 &&
                this.adapter.getAttributeWidth(collection) >
                    this.adapter.getDocumentSizeLimit()
            ) {
                throw new LimitException(
                    `Document size limit of ${this.adapter.getDocumentSizeLimit()} exceeded. Cannot create collection.`,
                );
            }
        }

        await this.adapter.createCollection(id, attributes, indexes);

        if (id === Database.METADATA) {
            return new Document<any>(Database.COLLECTION);
        }

        const createdCollection = await this.silent(
            async () =>
                await this.createDocument<any>(Database.METADATA, collection),
        );

        await this.trigger(Database.EVENT_COLLECTION_CREATE, createdCollection);

        return createdCollection;
    }

    /**
     * Update Collections Permissions.
     *
     * @return Promise<Document>
     * @throws ConflictException
     * @throws DatabaseException
     */
    public async updateCollection<T extends IRecord>(
        params: UpdateCollectionParams,
    ): Promise<Document<T>>;
    public async updateCollection<T extends IRecord>(
        id: string,
        permissions: string[],
        documentSecurity: boolean,
    ): Promise<Document<T>>;
    public async updateCollection<T extends IRecord>(
        idOrParams: string | UpdateCollectionParams,
        permissions?: string[],
        documentSecurity?: boolean,
    ): Promise<Document<T>> {
        let id: string;

        if (typeof idOrParams === "string") {
            id = idOrParams;
        } else {
            id = idOrParams.id;
            permissions = idOrParams.permissions;
            documentSecurity = idOrParams.documentSecurity;
        }

        if (this.validate) {
            const validator = new Permissions();
            if (!validator.isValid(permissions)) {
                throw new DatabaseError(validator.getDescription());
            }
        }

        let collection = await this.silent(
            async () => await this.getCollection<any>(id),
        );

        if (collection.isEmpty()) {
            throw new NotFoundException("Collection not found");
        }

        if (
            this.adapter.getSharedTables() &&
            collection.getAttribute("$tenant") != this.adapter.getTenantId()
        ) {
            throw new NotFoundException("Collection not found");
        }

        collection
            .setAttribute("$permissions", permissions)
            .setAttribute("documentSecurity", documentSecurity);

        collection = await this.silent(
            async () =>
                await this.updateDocument(
                    Database.METADATA,
                    collection.getId(),
                    collection,
                ),
        );

        await this.trigger(Database.EVENT_COLLECTION_UPDATE, collection);

        return collection;
    }

    /**
     * Get Collection
     *
     * @param id
     * @return Promise<Document>
     * @throws DatabaseException
     */
    public async getCollection<T extends IRecord>(
        id: string,
    ): Promise<Document<T>> {
        const collection = await this.silent(
            async () =>
                await this.getDocument<{ $tenant: number } & any>(
                    Database.METADATA,
                    id,
                ),
        );
        const tenant = collection.getAttribute("$tenant");

        if (
            id !== Database.METADATA &&
            this.adapter.getSharedTables() &&
            tenant !== null &&
            tenant != this.adapter.getTenantId()
        ) {
            return new Document();
        }

        await this.trigger(Database.EVENT_COLLECTION_READ, collection);

        return collection;
    }

    /**
     * List Collections
     *
     * @param limit
     * @param offset
     * @return Promise<Document[]>
     * @throws DatabaseException
     */
    public async listCollections(
        limit: number = 25,
        offset: number = 0,
    ): Promise<Document[]> {
        const result = await this.silent(
            async () =>
                await this.find(Database.METADATA, [
                    Query.limit(limit),
                    Query.offset(offset),
                ]),
        );

        await this.trigger(Database.EVENT_COLLECTION_LIST, result);

        return result;
    }

    /**
     * Get Collection Size
     *
     * @param collection
     * @return Promise<number>
     * @throws DatabaseException
     */
    public async getSizeOfCollection(collection: string): Promise<number> {
        const col = await this.silent(
            async () => await this.getCollection(collection),
        );

        if (col.isEmpty()) {
            throw new NotFoundException("Collection not found");
        }

        if (
            this.adapter.getSharedTables() &&
            col.getAttribute("$tenant") != this.adapter.getTenantId()
        ) {
            throw new NotFoundException("Collection not found");
        }

        return await this.adapter.getSizeOfCollection(col.getId());
    }

    /**
     * Get Collection Size on disk
     *
     * @param collection
     * @return Promise<number>
     * @throws DatabaseException
     */
    public async getSizeOfCollectionOnDisk(
        collection: string,
    ): Promise<number> {
        if (this.adapter.getSharedTables() && !this.adapter.getTenantId()) {
            throw new DatabaseError(
                "Missing tenant. Tenant must be set when table sharing is enabled.",
            );
        }

        const col = await this.silent(
            async () => await this.getCollection(collection),
        );

        if (col.isEmpty()) {
            throw new NotFoundException("Collection not found");
        }

        if (
            this.adapter.getSharedTables() &&
            col.getAttribute("$tenant") != this.adapter.getTenantId()
        ) {
            throw new NotFoundException("Collection not found");
        }

        return await this.adapter.getSizeOfCollectionOnDisk(col.getId());
    }

    /**
     * Delete Collection
     *
     * @param id
     * @return Promise<boolean>
     * @throws DatabaseException
     */
    public async deleteCollection(id: string): Promise<boolean> {
        const collection = await this.silent(
            async () => await this.getDocument(Database.METADATA, id),
        );

        if (collection.isEmpty()) {
            throw new NotFoundException("Collection not found");
        }

        if (
            this.adapter.getSharedTables() &&
            collection.getAttribute("$tenant") != this.adapter.getTenantId()
        ) {
            throw new NotFoundException("Collection not found");
        }

        const relationships = collection
            .getAttribute("attributes")
            .filter(
                (attribute: any) =>
                    attribute.type === Database.VAR_RELATIONSHIP,
            );

        for (const relationship of relationships) {
            await this.deleteRelationship(collection.getId(), relationship.$id);
        }

        await this.adapter.dropCollection(id);

        let deleted: boolean;
        if (id === Database.METADATA) {
            deleted = true;
        } else {
            deleted = await this.silent(
                async () => await this.deleteDocument(Database.METADATA, id),
            );
        }

        if (deleted) {
            await this.trigger(Database.EVENT_COLLECTION_DELETE, collection);
        }

        await this.purgeCachedCollection(id);

        return deleted;
    }

    /**
     * Create Attribute
     *
     * @return Promise<boolean>
     * @throws AuthorizationException
     * @throws ConflictException
     * @throws DatabaseException
     * @throws DuplicateException
     * @throws LimitException
     * @throws StructureException
     * @throws Error
     */
    public async createAttribute(
        params: CreateAttributeParams,
    ): Promise<boolean>;
    public async createAttribute(
        collection: string,
        id: string,
        type: string,
        size: number,
        required: boolean,
        defaultValue?: any,
        signed?: boolean,
        array?: boolean,
        format?: string | null,
        formatOptions?: Record<string, any>,
        filters?: string[],
    ): Promise<boolean>;
    public async createAttribute(
        collectionOrParams: string | CreateAttributeParams,
        id?: string,
        type?: string,
        size?: number,
        required?: boolean,
        defaultValue: any = null,
        signed: boolean = true,
        array: boolean = false,
        format: string | null = null,
        formatOptions: Record<string, any> = {},
        filters: string[] = [],
    ): Promise<boolean> {
        let collection: string;

        if (typeof collectionOrParams === "string") {
            collection = collectionOrParams;
        } else {
            collection = collectionOrParams.collection;
            id = collectionOrParams.id;
            type = collectionOrParams.type;
            size = collectionOrParams.size;
            required = collectionOrParams.required;
            defaultValue = collectionOrParams.defaultValue ?? null;
            signed = collectionOrParams.signed ?? true;
            array = collectionOrParams.array ?? false;
            format = collectionOrParams.format ?? null;
            formatOptions = collectionOrParams.formatOptions ?? {};
            filters = collectionOrParams.filters ?? [];
        }

        const col = await this.silent(
            async () => await this.getCollection(collection),
        );

        if (col.isEmpty()) {
            throw new NotFoundException("Collection not found");
        }

        // Attribute IDs are case-insensitive
        const attributes = col.getAttribute("attributes", []);
        for (const attribute of attributes) {
            if (
                attribute.getAttribute("$id", "").toLowerCase() ===
                id!.toLowerCase()
            ) {
                throw new DuplicateException("Attribute already exists");
            }
        }

        // Ensure required filters for the attribute are passed
        const requiredFilters = this.getRequiredFilters(type!);
        if (requiredFilters.some((filter: any) => !filters.includes(filter))) {
            throw new DatabaseException(
                `Attribute of type: ${type} requires the following filters: ${requiredFilters.join(",")}`,
            );
        }

        if (
            this.adapter.getLimitForAttributes() > 0 &&
            this.adapter.getCountOfAttributes(col) >=
                this.adapter.getLimitForAttributes()
        ) {
            throw new LimitException(
                "Column limit reached. Cannot create new attribute.",
            );
        }

        if (format && !Structure.hasFormat(format, type!)) {
            throw new DatabaseException(
                `Format ("${format}") not available for this attribute type ("${type}")`,
            );
        }

        const attribute = new Document({
            $id: ID.custom(id!),
            key: id,
            type: type,
            size: size,
            required: required,
            default: defaultValue,
            signed: signed,
            array: array,
            format: format,
            formatOptions: formatOptions,
            filters: filters,
        });

        col.setAttribute("attributes", attribute, Document.SET_TYPE_APPEND);

        if (
            this.adapter.getDocumentSizeLimit() > 0 &&
            this.adapter.getAttributeWidth(col) >=
                this.adapter.getDocumentSizeLimit()
        ) {
            throw new LimitException(
                "Row width limit reached. Cannot create new attribute.",
            );
        }

        switch (type) {
            case Database.VAR_STRING:
                if (size! > this.adapter.getLimitForString()) {
                    throw new DatabaseException(
                        `Max size allowed for string is: ${this.adapter.getLimitForString()}`,
                    );
                }
                break;
            case Database.VAR_INTEGER:
                const limit = signed
                    ? this.adapter.getLimitForInt() / 2
                    : this.adapter.getLimitForInt();
                if (size! > limit) {
                    throw new DatabaseException(
                        `Max size allowed for int is: ${limit}`,
                    );
                }
                break;
            case Database.VAR_FLOAT:
            case Database.VAR_BOOLEAN:
            case Database.VAR_DATETIME:
            case Database.VAR_RELATIONSHIP:
                break;
            default:
                throw new DatabaseException(
                    `Unknown attribute type: ${type}. Must be one of ${Database.VAR_STRING}, ${Database.VAR_INTEGER}, ${Database.VAR_FLOAT}, ${Database.VAR_BOOLEAN}, ${Database.VAR_DATETIME}, ${Database.VAR_RELATIONSHIP}`,
                );
        }

        if (defaultValue !== null) {
            if (required) {
                throw new DatabaseException(
                    "Cannot set a default value for a required attribute",
                );
            }
            this.validateDefaultTypes(type, defaultValue);
        }

        try {
            const created = await this.adapter.createAttribute(
                col.getId(),
                id!,
                type,
                size!,
                signed,
                array,
            );
            if (!created) {
                throw new DatabaseException("Failed to create attribute");
            }
        } catch (e) {
            if (
                !(e instanceof DuplicateException) ||
                !this.adapter.getSharedTables() ||
                !this.isMigrating()
            ) {
                throw e;
            }
        }

        if (col.getId() !== Database.METADATA) {
            await this.silent(
                async () =>
                    await this.updateDocument(
                        Database.METADATA,
                        col.getId(),
                        col,
                    ),
            );
        }

        await this.purgeCachedCollection(col.getId());
        await this.purgeCachedDocument(Database.METADATA, col.getId());

        await this.trigger(Database.EVENT_ATTRIBUTE_CREATE, attribute);

        return true;
    }

    /**
     * Get the list of required filters for each data type
     *
     * @param type Type of the attribute
     * @return string[]
     */
    protected getRequiredFilters(type: string | null): string[] {
        switch (type) {
            case Database.VAR_DATETIME:
                return ["datetime"];
            default:
                return [];
        }
    }

    /**
     * Function to validate if the default value of an attribute matches its attribute type
     *
     * @param type Type of the attribute
     * @param defaultValue Default value of the attribute
     * @throws DatabaseException
     */
    protected validateDefaultTypes(type: string, defaultValue: any): void {
        const defaultType = typeof defaultValue;

        if (defaultType === "undefined" || defaultValue === null) {
            // Disable null. No validation required
            return;
        }

        if (Array.isArray(defaultValue)) {
            for (const value of defaultValue) {
                this.validateDefaultTypes(type, value);
            }
            return;
        }

        switch (type) {
            case Database.VAR_STRING:
            case Database.VAR_INTEGER:
            case Database.VAR_FLOAT:
            case Database.VAR_BOOLEAN:
                if (type !== defaultType) {
                    throw new DatabaseException(
                        `Default value ${defaultValue} does not match given type ${type}`,
                    );
                }
                break;
            case Database.VAR_DATETIME:
                if (defaultType !== "string") {
                    throw new DatabaseException(
                        `Default value ${defaultValue} does not match given type ${type}`,
                    );
                }
                break;
            default:
                throw new DatabaseException(
                    `Unknown attribute type: ${type}. Must be one of ${Database.VAR_STRING}, ${Database.VAR_INTEGER}, ${Database.VAR_FLOAT}, ${Database.VAR_BOOLEAN}, ${Database.VAR_DATETIME}, ${Database.VAR_RELATIONSHIP}`,
                );
        }
    }

    /**
     * Update attribute metadata. Utility method for update attribute methods.
     *
     * @param collection
     * @param id
     * @param updateCallback method that receives document, and returns it with changes applied
     * @return Document
     * @throws ConflictException
     * @throws DatabaseException
     */
    protected async updateIndexMeta(
        collection: string,
        id: string,
        updateCallback: (
            index: Document,
            collection: Document,
            indexPosition: number,
        ) => void | Promise<void>,
    ): Promise<Document> {
        const col = await this.silent(
            async () => await this.getCollection(collection),
        );

        if (col.getId() === Database.METADATA) {
            throw new DatabaseException("Cannot update metadata indexes");
        }

        const indexes = col.getAttribute("indexes", []);
        const indexPosition = indexes.findIndex(
            (index: any) => index.getId() === id,
        );

        if (indexPosition === -1) {
            throw new NotFoundException("Index not found");
        }

        // Execute update from callback
        await updateCallback(indexes[indexPosition], col, indexPosition);

        // Save
        col.setAttribute("indexes", indexes);

        await this.silent(
            async () =>
                await this.updateDocument(Database.METADATA, col.getId(), col),
        );

        await this.trigger(
            Database.EVENT_ATTRIBUTE_UPDATE,
            indexes[indexPosition],
        );

        return indexes[indexPosition];
    }

    /**
     * Update attribute metadata. Utility method for update attribute methods.
     *
     * @param collection
     * @param id
     * @param updateCallback method that receives document, and returns it with changes applied
     * @return Document
     * @throws ConflictException
     * @throws DatabaseException
     */
    protected async updateAttributeMeta(
        collection: string,
        id: string,
        updateCallback: (
            attribute: Document,
            collection: Document,
            index: number,
        ) => void | Promise<void>,
    ): Promise<Document> {
        const col = await this.silent(
            async () => await this.getCollection(collection),
        );

        if (col.getId() === Database.METADATA) {
            throw new DatabaseException("Cannot update metadata attributes");
        }

        const attributes = col.getAttribute("attributes", []);
        const index = attributes.findIndex(
            (attribute: any) => attribute.getId() === id,
        );

        if (index === -1) {
            throw new NotFoundException("Attribute not found");
        }

        // Execute update from callback
        await updateCallback(attributes[index], col, index);

        // Save
        col.setAttribute("attributes", attributes);

        await this.silent(
            async () =>
                await this.updateDocument(Database.METADATA, col.getId(), col),
        );

        await this.trigger(Database.EVENT_ATTRIBUTE_UPDATE, attributes[index]);

        return attributes[index];
    }

    /**
     * Update required status of attribute.
     *
     * @param collection string
     * @param id string
     * @param required boolean
     *
     * @return Promise<Document>
     * @throws Error
     */
    public async updateAttributeRequired(
        collection: string,
        id: string,
        required: boolean,
    ): Promise<Document> {
        return await this.updateAttributeMeta(collection, id, (attribute) => {
            attribute.setAttribute("required", required);
        });
    }

    /**
     * Update format of attribute.
     *
     * @param collection string
     * @param id string
     * @param format string validation format of attribute
     *
     * @return Promise<Document>
     * @throws Error
     */
    public async updateAttributeFormat(
        collection: string,
        id: string,
        format: string,
    ): Promise<Document> {
        return await this.updateAttributeMeta(collection, id, (attribute) => {
            if (!Structure.hasFormat(format, attribute.getAttribute("type"))) {
                throw new DatabaseException(
                    `Format "${format}" not available for attribute type "${attribute.getAttribute("type")}"`,
                );
            }

            attribute.setAttribute("format", format);
        });
    }

    /**
     * Update format options of attribute.
     *
     * @param collection string
     * @param id string
     * @param formatOptions Record<string, any> assoc array with custom options that can be passed for the format validation
     *
     * @return Promise<Document>
     * @throws Error
     */
    public async updateAttributeFormatOptions(
        collection: string,
        id: string,
        formatOptions: Record<string, any>,
    ): Promise<Document> {
        return await this.updateAttributeMeta(collection, id, (attribute) => {
            attribute.setAttribute("formatOptions", formatOptions);
        });
    }

    /**
     * Update filters of attribute.
     *
     * @param collection string
     * @param id string
     * @param filters string[]
     *
     * @return Promise<Document>
     * @throws Error
     */
    public async updateAttributeFilters(
        collection: string,
        id: string,
        filters: string[],
    ): Promise<Document> {
        return await this.updateAttributeMeta(collection, id, (attribute) => {
            attribute.setAttribute("filters", filters);
        });
    }

    /**
     * Update default value of attribute
     *
     * @param collection string
     * @param id string
     * @param defaultValue any
     *
     * @return Promise<Document>
     * @throws Error
     */
    public async updateAttributeDefault(
        collection: string,
        id: string,
        defaultValue: any = null,
    ): Promise<Document> {
        return await this.updateAttributeMeta(collection, id, (attribute) => {
            if (attribute.getAttribute("required") === true) {
                throw new DatabaseException(
                    "Cannot set a default value on a required attribute",
                );
            }

            this.validateDefaultTypes(
                attribute.getAttribute("type"),
                defaultValue,
            );

            attribute.setAttribute("default", defaultValue);
        });
    }

    /**
     * Update Attribute. This method is for updating data that causes underlying structure to change. Check out other updateAttribute methods if you are looking for metadata adjustments.
     *
     * @return Promise<Document>
     * @throws Error
     */
    public async updateAttribute(
        params: UpdateAttributeParams,
    ): Promise<Document>;
    public async updateAttribute(
        collection: string,
        id: string,
        type?: string | null,
        size?: number | null,
        required?: boolean | null,
        defaultValue?: any,
        signed?: boolean | null,
        array?: boolean | null,
        format?: string | null,
        formatOptions?: Record<string, any> | null,
        filters?: string[] | null,
        newKey?: string | null,
    ): Promise<Document>;
    public async updateAttribute(
        collectionOrParams: string | UpdateAttributeParams,
        id?: string,
        type: string | null = null,
        size: number | null = null,
        required: boolean | null = null,
        defaultValue: any = null,
        signed: boolean | null = null,
        array: boolean | null = null,
        format: string | null = null,
        formatOptions: Record<string, any> | null = null,
        filters: string[] | null = null,
        newKey: string | null = null,
    ): Promise<Document> {
        let collection: string;

        if (typeof collectionOrParams === "string") {
            collection = collectionOrParams;
        } else {
            collection = collectionOrParams.collection;
            id = collectionOrParams.id;
            type = collectionOrParams.type ?? null;
            size = collectionOrParams.size ?? null;
            required = collectionOrParams.required ?? null;
            defaultValue = collectionOrParams.defaultValue ?? null;
            signed = collectionOrParams.signed ?? null;
            array = collectionOrParams.array ?? null;
            format = collectionOrParams.format ?? null;
            formatOptions = collectionOrParams.formatOptions ?? null;
            filters = collectionOrParams.filters ?? null;
            newKey = collectionOrParams.newKey ?? null;
        }

        return await this.updateAttributeMeta(
            collection,
            id!,
            async (attribute, collectionDoc, attributeIndex) => {
                const altering =
                    type !== null ||
                    size !== null ||
                    signed !== null ||
                    array !== null ||
                    newKey !== null;
                type = type ?? attribute.getAttribute("type");
                size = size ?? attribute.getAttribute("size");
                signed = signed ?? attribute.getAttribute("signed");
                required = required ?? attribute.getAttribute("required");
                defaultValue =
                    defaultValue ?? attribute.getAttribute("default");
                array = array ?? attribute.getAttribute("array");
                format = format ?? attribute.getAttribute("format");
                formatOptions =
                    formatOptions ?? attribute.getAttribute("formatOptions");
                filters = filters ?? attribute.getAttribute("filters");

                if (required === true && defaultValue !== null) {
                    defaultValue = null;
                }

                switch (type) {
                    case "string":
                        if (!size) {
                            throw new DatabaseException(
                                "Size length is required",
                            );
                        }

                        if (size > this.adapter.getLimitForString()) {
                            throw new DatabaseException(
                                "Max size allowed for string is: " +
                                    this.adapter.getLimitForString(),
                            );
                        }
                        break;

                    case "integer":
                        const limit = signed
                            ? this.adapter.getLimitForInt() / 2
                            : this.adapter.getLimitForInt();
                        if (size && size > limit) {
                            throw new DatabaseException(
                                "Max size allowed for int is: " + limit,
                            );
                        }
                        break;
                    case "float":
                    case "boolean":
                    case "datetime":
                        if (size) {
                            throw new DatabaseException("Size must be empty");
                        }
                        break;
                    default:
                        throw new DatabaseException(
                            "Unknown attribute type: " +
                                type +
                                ". Must be one of string, integer, float, boolean, datetime, relationship",
                        );
                }

                const requiredFilters = this.getRequiredFilters(type);
                if (
                    requiredFilters.some((filter) => !filters?.includes(filter))
                ) {
                    throw new DatabaseException(
                        `Attribute of type: ${type} requires the following filters: ${requiredFilters.join(",")}`,
                    );
                }

                if (format) {
                    if (!Structure.hasFormat(format, type)) {
                        throw new DatabaseException(
                            `Format ("${format}") not available for this attribute type ("${type}")`,
                        );
                    }
                }

                if (defaultValue !== null) {
                    if (required) {
                        throw new DatabaseException(
                            "Cannot set a default value on a required attribute",
                        );
                    }

                    this.validateDefaultTypes(type, defaultValue);
                }

                attribute
                    .setAttribute("$id", newKey ?? id)
                    .setAttribute("key", newKey ?? id)
                    .setAttribute("type", type)
                    .setAttribute("size", size)
                    .setAttribute("signed", signed)
                    .setAttribute("array", array)
                    .setAttribute("format", format)
                    .setAttribute("formatOptions", formatOptions)
                    .setAttribute("filters", filters)
                    .setAttribute("required", required)
                    .setAttribute("default", defaultValue);

                const attributes = collectionDoc.getAttribute("attributes");
                attributes[attributeIndex] = attribute;
                collectionDoc.setAttribute("attributes", attributes);

                if (
                    this.adapter.getDocumentSizeLimit() > 0 &&
                    this.adapter.getAttributeWidth(collectionDoc) >=
                        this.adapter.getDocumentSizeLimit()
                ) {
                    throw new LimitException(
                        "Row width limit reached. Cannot update attribute.",
                    );
                }

                if (altering) {
                    const indexes = collectionDoc.getAttribute("indexes");

                    if (newKey && id !== newKey) {
                        for (const index of indexes) {
                            if (index.attributes.includes(id)) {
                                index.attributes = index.attributes.map(
                                    (attr: any) =>
                                        attr === id ? newKey : attr,
                                );
                            }
                        }
                    }

                    if (this.validate) {
                        const validator = new IndexValidator(
                            attributes,
                            this.adapter.getMaxIndexLength(),
                            this.adapter.getInternalIndexesKeys(),
                        );

                        for (const index of indexes) {
                            if (!validator.isValid(index)) {
                                throw new DatabaseException(
                                    validator.getDescription(),
                                );
                            }
                        }
                    }

                    const updated = await this.adapter.updateAttribute(
                        collection,
                        id!,
                        type,
                        size as any,
                        signed as any,
                        array as any,
                        newKey as any,
                    );

                    if (!updated) {
                        throw new DatabaseException(
                            "Failed to update attribute",
                        );
                    }

                    await this.purgeCachedCollection(collection);
                }

                await this.purgeCachedDocument(Database.METADATA, collection);
            },
        );
    }

    /**
     * Checks if attribute can be added to collection.
     * Used to check attribute limits without asking the database
     * Returns true if attribute can be added to collection, throws exception otherwise
     *
     * @param collection
     * @param attribute
     *
     * @throws LimitException
     * @return boolean
     */
    public checkAttribute(collection: Document, attribute: Document): boolean {
        const clonedCollection = collection.clone();

        clonedCollection.setAttribute(
            "attributes",
            attribute,
            Document.SET_TYPE_APPEND,
        );

        if (
            this.adapter.getLimitForAttributes() > 0 &&
            this.adapter.getCountOfAttributes(clonedCollection) >
                this.adapter.getLimitForAttributes()
        ) {
            throw new LimitException(
                "Column limit reached. Cannot create new attribute.",
            );
        }

        if (
            this.adapter.getDocumentSizeLimit() > 0 &&
            this.adapter.getAttributeWidth(clonedCollection) >=
                this.adapter.getDocumentSizeLimit()
        ) {
            throw new LimitException(
                "Row width limit reached. Cannot create new attribute.",
            );
        }

        return true;
    }

    /**
     * Delete Attribute
     *
     * @param collection
     * @param id
     *
     * @return Promise<boolean>
     * @throws ConflictException
     * @throws DatabaseException
     */
    public async deleteAttribute(
        collection: string,
        id: string,
    ): Promise<boolean> {
        const col = await this.silent(
            async () => await this.getCollection(collection),
        );
        const attributes = col.getAttribute("attributes", []);
        const indexes = col.getAttribute("indexes", []);

        let attribute: Document | null = null;

        for (let i = 0; i < attributes.length; i++) {
            if (attributes[i].getAttribute("$id") === id) {
                attribute = attributes[i];
                attributes.splice(i, 1);
                break;
            }
        }

        if (!attribute) {
            throw new NotFoundException("Attribute not found");
        }

        if (
            (attribute as Document).getAttribute("type") ===
            Database.VAR_RELATIONSHIP
        ) {
            throw new DatabaseException(
                "Cannot delete relationship as an attribute",
            );
        }

        for (let i = 0; i < indexes.length; i++) {
            const indexAttributes = indexes[i]
                .getAttribute("attributes", [])
                .filter((attr: string) => attr !== id);

            if (indexAttributes.length === 0) {
                indexes.splice(i, 1);
            } else {
                indexes[i].setAttribute("attributes", indexAttributes);
            }
        }

        const deleted = await this.adapter.deleteAttribute(col.getId(), id);

        if (!deleted) {
            throw new DatabaseException("Failed to delete attribute");
        }

        col.setAttribute("attributes", attributes);
        col.setAttribute("indexes", indexes);

        if (col.getId() !== Database.METADATA) {
            await this.silent(
                async () =>
                    await this.updateDocument(
                        Database.METADATA,
                        col.getId(),
                        col,
                    ),
            );
        }

        await this.purgeCachedCollection(col.getId());
        await this.purgeCachedDocument(Database.METADATA, col.getId());

        await this.trigger(Database.EVENT_ATTRIBUTE_DELETE, attribute);

        return true;
    }

    /**
     * Rename Attribute
     *
     * @param collection string
     * @param oldId string Current attribute ID
     * @param newId string
     * @return Promise<boolean>
     * @throws AuthorizationException
     * @throws ConflictException
     * @throws DatabaseException
     * @throws DuplicateException
     * @throws StructureException
     */
    public async renameAttribute(
        collection: string,
        oldId: string,
        newId: string,
    ): Promise<boolean> {
        const col = await this.silent(
            async () => await this.getCollection(collection),
        );

        const attributes = col.getAttribute("attributes", []);
        const indexes = col.getAttribute("indexes", []);

        let attribute = null;

        for (const value of attributes) {
            if (value.getId() === oldId) {
                attribute = value;
            }

            if (value.getId() === newId) {
                throw new DuplicateException("Attribute name already used");
            }
        }

        if (!attribute) {
            throw new NotFoundException("Attribute not found");
        }

        attribute.setAttribute("$id", newId);
        attribute.setAttribute("key", newId);

        for (const index of indexes) {
            const indexAttributes = index.getAttribute("attributes", []);
            index.setAttribute(
                "attributes",
                indexAttributes.map((attr: any) =>
                    attr === oldId ? newId : attr,
                ),
            );
        }

        const renamed = await this.adapter.renameAttribute(
            col.getId(),
            oldId,
            newId,
        );

        col.setAttribute("attributes", attributes);
        col.setAttribute("indexes", indexes);

        if (col.getId() !== Database.METADATA) {
            await this.silent(
                async () =>
                    await this.updateDocument(
                        Database.METADATA,
                        col.getId(),
                        col,
                    ),
            );
        }

        await this.trigger(Database.EVENT_ATTRIBUTE_UPDATE, attribute);

        return renamed;
    }

    /**
     * Create a relationship attribute
     *
     * @return Promise<boolean>
     * @throws AuthorizationException
     * @throws ConflictException
     * @throws DatabaseException
     * @throws DuplicateException
     * @throws LimitException
     * @throws StructureException
     */
    public async createRelationship(
        params: CreateRelationshipParams,
    ): Promise<boolean>;
    public async createRelationship(
        collection: string,
        relatedCollection: string,
        type: string,
        twoWay?: boolean,
        id?: string | null,
        twoWayKey?: string | null,
        onDelete?: string,
    ): Promise<boolean>;
    public async createRelationship(
        collectionOrParams: string | CreateRelationshipParams,
        relatedCollection?: string,
        type?: string,
        twoWay: boolean = false,
        id: string | null = null,
        twoWayKey: string | null = null,
        onDelete: string = Database.RELATION_MUTATE_RESTRICT,
    ): Promise<boolean> {
        let collection: string;

        if (typeof collectionOrParams === "string") {
            collection = collectionOrParams;
        } else {
            collection = collectionOrParams.collection;
            relatedCollection = collectionOrParams.relatedCollection;
            type = collectionOrParams.type;
            twoWay = collectionOrParams.twoWay ?? false;
            id = collectionOrParams.id ?? null;
            twoWayKey = collectionOrParams.twoWayKey ?? null;
            onDelete =
                collectionOrParams.onDelete ??
                Database.RELATION_MUTATE_RESTRICT;
        }

        const col = await this.silent(
            async () => await this.getCollection(collection),
        );

        if (col.isEmpty()) {
            throw new NotFoundException("Collection not found");
        }

        const relatedCol = await this.silent(
            async () => await this.getCollection(relatedCollection!),
        );

        if (relatedCol.isEmpty()) {
            throw new NotFoundException("Related collection not found");
        }

        id = id ?? relatedCol.getId();
        twoWayKey = twoWayKey ?? col.getId();

        const attributes = col.getAttribute("attributes", []);
        for (const attribute of attributes) {
            if (attribute.getId().toLowerCase() === id.toLowerCase()) {
                throw new DuplicateException("Attribute already exists");
            }

            if (
                attribute.getAttribute("type") === Database.VAR_RELATIONSHIP &&
                attribute.getAttribute("options").twoWayKey.toLowerCase() ===
                    twoWayKey.toLowerCase() &&
                attribute.getAttribute("options").relatedCollection ===
                    relatedCol.getId()
            ) {
                throw new DuplicateException(
                    "Related attribute already exists",
                );
            }
        }

        const relationship = new Document({
            $id: ID.custom(id),
            key: id,
            type: Database.VAR_RELATIONSHIP,
            required: false,
            default: null,
            options: {
                relatedCollection: relatedCol.getId(),
                relationType: type,
                twoWay: twoWay,
                twoWayKey: twoWayKey,
                onDelete: onDelete,
                side: Database.RELATION_SIDE_PARENT,
            },
        });

        const twoWayRelationship = new Document({
            $id: ID.custom(twoWayKey),
            key: twoWayKey,
            type: Database.VAR_RELATIONSHIP,
            required: false,
            default: null,
            options: {
                relatedCollection: col.getId(),
                relationType: type,
                twoWay: twoWay,
                twoWayKey: id,
                onDelete: onDelete,
                side: Database.RELATION_SIDE_CHILD,
            },
        });

        this.checkAttribute(col, relationship);
        this.checkAttribute(relatedCol, twoWayRelationship);

        col.setAttribute("attributes", relationship, Document.SET_TYPE_APPEND);
        relatedCol.setAttribute(
            "attributes",
            twoWayRelationship,
            Document.SET_TYPE_APPEND,
        );

        if (type === Database.RELATION_MANY_TO_MANY) {
            await this.silent(
                async () =>
                    await this.createCollection(
                        `_${col.getInternalId()}_${relatedCol.getInternalId()}`,
                        [
                            new Document({
                                $id: id,
                                key: id,
                                type: Database.VAR_STRING,
                                size: Database.LENGTH_KEY,
                                required: true,
                                signed: true,
                                array: false,
                                filters: [],
                            }),
                            new Document({
                                $id: twoWayKey,
                                key: twoWayKey,
                                type: Database.VAR_STRING,
                                size: Database.LENGTH_KEY,
                                required: true,
                                signed: true,
                                array: false,
                                filters: [],
                            }),
                        ],
                        [
                            new Document({
                                $id: `_index_${id}`,
                                key: `index_${id}`,
                                type: Database.INDEX_KEY,
                                attributes: [id],
                            }),
                            new Document({
                                $id: `_index_${twoWayKey}`,
                                key: `_index_${twoWayKey}`,
                                type: Database.INDEX_KEY,
                                attributes: [twoWayKey],
                            }),
                        ],
                    ),
            );
        }

        const created = await this.adapter.createRelationship(
            col.getId(),
            relatedCol.getId(),
            type!,
            twoWay,
            id,
            twoWayKey,
        );

        if (!created) {
            throw new DatabaseException("Failed to create relationship");
        }

        await this.silent(async () => {
            await this.updateDocument(Database.METADATA, col.getId(), col);
            await this.updateDocument(
                Database.METADATA,
                relatedCol.getId(),
                relatedCol,
            );

            const indexKey = `_index_${id}`;
            const twoWayIndexKey = `_index_${twoWayKey}`;

            switch (type) {
                case Database.RELATION_ONE_TO_ONE:
                    await this.createIndex(
                        col.getId(),
                        indexKey,
                        Database.INDEX_UNIQUE,
                        [id],
                    );
                    if (twoWay) {
                        await this.createIndex(
                            relatedCol.getId(),
                            twoWayIndexKey,
                            Database.INDEX_UNIQUE,
                            [twoWayKey],
                        );
                    }
                    break;
                case Database.RELATION_ONE_TO_MANY:
                    await this.createIndex(
                        relatedCol.getId(),
                        twoWayIndexKey,
                        Database.INDEX_KEY,
                        [twoWayKey],
                    );
                    break;
                case Database.RELATION_MANY_TO_ONE:
                    await this.createIndex(
                        col.getId(),
                        indexKey,
                        Database.INDEX_KEY,
                        [id],
                    );
                    break;
                case Database.RELATION_MANY_TO_MANY:
                    // Indexes created on junction collection creation
                    break;
                default:
                    throw new RelationshipException(
                        "Invalid relationship type.",
                    );
            }
        });

        await this.trigger(Database.EVENT_ATTRIBUTE_CREATE, relationship);

        return true;
    }

    /**
     * Update a relationship attribute
     *
     * @return Promise<boolean>
     * @throws ConflictException
     * @throws DatabaseException
     */
    public async updateRelationship(
        params: UpdateRelationshipParams,
    ): Promise<boolean>;
    public async updateRelationship(
        collection: string,
        id: string,
        newKey?: string | null,
        newTwoWayKey?: string | null,
        twoWay?: boolean | null,
        onDelete?: string | null,
    ): Promise<boolean>;
    public async updateRelationship(
        collectionOrParams: string | UpdateRelationshipParams,
        id?: string,
        newKey: string | null = null,
        newTwoWayKey: string | null = null,
        twoWay: boolean | null = null,
        onDelete: string | null = null,
    ): Promise<boolean> {
        let collection: string;

        if (typeof collectionOrParams === "string") {
            collection = collectionOrParams;
        } else {
            collection = collectionOrParams.collection;
            id = collectionOrParams.id;
            newKey = collectionOrParams.newKey ?? null;
            newTwoWayKey = collectionOrParams.newTwoWayKey ?? null;
            twoWay = collectionOrParams.twoWay ?? null;
            onDelete = collectionOrParams.onDelete ?? null;
        }

        if (!newKey && !newTwoWayKey && !twoWay && !onDelete) {
            return true;
        }

        const col = await this.getCollection(collection);
        const attributes = col.getAttribute("attributes", []);

        if (
            newKey &&
            attributes.some((attr: any) => attr.getAttribute("key") === newKey)
        ) {
            throw new DuplicateException("Attribute already exists");
        }

        const attributeIndex = attributes.findIndex(
            (attr: any) => attr.getId() === id,
        );

        if (attributeIndex === -1) {
            throw new NotFoundException("Attribute not found");
        }

        const attribute = attributes[attributeIndex];
        const type = attribute.getAttribute("options").relationType;
        const side = attribute.getAttribute("options").side;

        const relatedCollectionId =
            attribute.getAttribute("options").relatedCollection;
        const relatedCollection = await this.getCollection(relatedCollectionId);

        await this.updateAttributeMeta(
            col.getId(),
            id!,
            async (attribute: any) => {
                const altering =
                    (newKey && newKey !== id) ||
                    (newTwoWayKey &&
                        newTwoWayKey !==
                            attribute.getAttribute("options").twoWayKey);

                const relatedAttributes = relatedCollection.getAttribute(
                    "attributes",
                    [],
                );

                if (
                    newTwoWayKey &&
                    relatedAttributes.some(
                        (attr: any) =>
                            attr.getAttribute("key") === newTwoWayKey,
                    )
                ) {
                    throw new DuplicateException(
                        "Related attribute already exists",
                    );
                }

                newKey = newKey ?? attribute.getAttribute("key");
                const twoWayKey = attribute.getAttribute("options").twoWayKey;
                newTwoWayKey =
                    newTwoWayKey ?? attribute.getAttribute("options").twoWayKey;
                twoWay = twoWay ?? attribute.getAttribute("options").twoWay;
                onDelete =
                    onDelete ?? attribute.getAttribute("options").onDelete;

                attribute.setAttribute("$id", newKey);
                attribute.setAttribute("key", newKey);
                attribute.setAttribute("options", {
                    relatedCollection: relatedCollection.getId(),
                    relationType: type,
                    twoWay: twoWay,
                    twoWayKey: newTwoWayKey,
                    onDelete: onDelete,
                    side: side,
                });

                await this.updateAttributeMeta(
                    relatedCollection.getId(),
                    twoWayKey,
                    async (twoWayAttribute) => {
                        const options = twoWayAttribute.getAttribute(
                            "options",
                            {},
                        );
                        options.twoWayKey = newKey;
                        options.twoWay = twoWay;
                        options.onDelete = onDelete;

                        twoWayAttribute.setAttribute("$id", newTwoWayKey);
                        twoWayAttribute.setAttribute("key", newTwoWayKey);
                        twoWayAttribute.setAttribute("options", options);
                    },
                );

                if (type === Database.RELATION_MANY_TO_MANY) {
                    const junction = this.getJunctionCollection(
                        col,
                        relatedCollection,
                        side,
                    );

                    await this.updateAttributeMeta(
                        junction,
                        id!,
                        async (junctionAttribute) => {
                            junctionAttribute.setAttribute("$id", newKey);
                            junctionAttribute.setAttribute("key", newKey);
                        },
                    );
                    await this.updateAttributeMeta(
                        junction,
                        twoWayKey,
                        async (junctionAttribute) => {
                            junctionAttribute.setAttribute("$id", newTwoWayKey);
                            junctionAttribute.setAttribute("key", newTwoWayKey);
                        },
                    );

                    await this.purgeCachedCollection(junction);
                }

                if (altering) {
                    const updated = await this.adapter.updateRelationship(
                        col.getId(),
                        relatedCollection.getId(),
                        type,
                        twoWay as any,
                        id!,
                        twoWayKey,
                        side,
                        newKey as any,
                        newTwoWayKey as any,
                    );

                    if (!updated) {
                        throw new DatabaseException(
                            "Failed to update relationship",
                        );
                    }
                }
            },
        );

        const renameIndex = async (
            collection: string,
            key: string,
            newKey: string,
        ) => {
            await this.updateIndexMeta(
                collection,
                `_index_${key}`,
                async (index) => {
                    index.setAttribute("attributes", [newKey]);
                },
            );
            await this.silent(
                async () =>
                    await this.renameIndex(
                        collection,
                        `_index_${key}`,
                        `_index_${newKey}`,
                    ),
            );
        };

        newKey = newKey ?? (attribute.getAttribute("key") as string);
        const options = attribute.getAttribute("options");
        const twoWayKey = options.twoWayKey;
        newTwoWayKey = newTwoWayKey ?? (options.twoWayKey as string);
        twoWay = twoWay ?? options.twoWay;
        onDelete = onDelete ?? options.onDelete;

        switch (type) {
            case Database.RELATION_ONE_TO_ONE:
                if (id !== newKey) {
                    await renameIndex(col.getId(), id!, newKey);
                }
                if (twoWay && twoWayKey !== newTwoWayKey) {
                    await renameIndex(
                        relatedCollection.getId(),
                        twoWayKey,
                        newTwoWayKey,
                    );
                }
                break;
            case Database.RELATION_ONE_TO_MANY:
                if (side === Database.RELATION_SIDE_PARENT) {
                    if (twoWayKey !== newTwoWayKey) {
                        await renameIndex(
                            relatedCollection.getId(),
                            twoWayKey,
                            newTwoWayKey,
                        );
                    }
                } else {
                    if (id !== newKey) {
                        await renameIndex(col.getId(), id!, newKey);
                    }
                }
                break;
            case Database.RELATION_MANY_TO_ONE:
                if (side === Database.RELATION_SIDE_PARENT) {
                    if (id !== newKey) {
                        await renameIndex(col.getId(), id!, newKey);
                    }
                } else {
                    if (twoWayKey !== newTwoWayKey) {
                        await renameIndex(
                            relatedCollection.getId(),
                            twoWayKey,
                            newTwoWayKey,
                        );
                    }
                }
                break;
            case Database.RELATION_MANY_TO_MANY:
                const junction = this.getJunctionCollection(
                    col,
                    relatedCollection,
                    side,
                );

                if (id !== newKey) {
                    await renameIndex(junction, id!, newKey);
                }
                if (twoWayKey !== newTwoWayKey) {
                    await renameIndex(junction, twoWayKey, newTwoWayKey);
                }
                break;
            default:
                throw new RelationshipException("Invalid relationship type.");
        }

        await this.purgeCachedCollection(col.getId());
        await this.purgeCachedCollection(relatedCollection.getId());

        return true;
    }

    /**
     * Delete a relationship attribute
     *
     * @param collection string
     * @param id string
     * @return Promise<boolean>
     * @throws AuthorizationException
     * @throws ConflictException
     * @throws DatabaseException
     * @throws StructureException
     */
    public async deleteRelationship(
        collection: string,
        id: string,
    ): Promise<boolean> {
        const col = await this.silent(
            async () => await this.getCollection(collection),
        );
        const attributes = col.getAttribute("attributes", []);
        let relationship = null;

        for (const [name, attribute] of attributes.entries()) {
            if (attribute.getId() === id) {
                relationship = attribute;
                attributes.splice(name, 1);
                break;
            }
        }

        if (!relationship) {
            throw new NotFoundException("Attribute not found");
        }

        col.setAttribute("attributes", attributes);

        const relatedCollectionId =
            relationship.getAttribute("options").relatedCollection;
        const type = relationship.getAttribute("options").relationType;
        const twoWay = relationship.getAttribute("options").twoWay;
        const twoWayKey = relationship.getAttribute("options").twoWayKey;
        const side = relationship.getAttribute("options").side;

        const relatedCollection = await this.silent(
            async () => await this.getCollection(relatedCollectionId),
        );
        const relatedAttributes = relatedCollection.getAttribute(
            "attributes",
            [],
        );

        for (const [name, attribute] of relatedAttributes.entries()) {
            if (attribute.getId() === twoWayKey) {
                relatedAttributes.splice(name, 1);
                break;
            }
        }

        relatedCollection.setAttribute("attributes", relatedAttributes);

        await this.silent(async () => {
            await this.updateDocument(Database.METADATA, col.getId(), col);
            await this.updateDocument(
                Database.METADATA,
                relatedCollection.getId(),
                relatedCollection,
            );

            const indexKey = `_index_${id}`;
            const twoWayIndexKey = `_index_${twoWayKey}`;

            switch (type) {
                case Database.RELATION_ONE_TO_ONE:
                    if (side === Database.RELATION_SIDE_PARENT) {
                        await this.deleteIndex(col.getId(), indexKey);
                        if (twoWay) {
                            await this.deleteIndex(
                                relatedCollection.getId(),
                                twoWayIndexKey,
                            );
                        }
                    }
                    if (side === Database.RELATION_SIDE_CHILD) {
                        await this.deleteIndex(
                            relatedCollection.getId(),
                            twoWayIndexKey,
                        );
                        if (twoWay) {
                            await this.deleteIndex(col.getId(), indexKey);
                        }
                    }
                    break;
                case Database.RELATION_ONE_TO_MANY:
                    if (side === Database.RELATION_SIDE_PARENT) {
                        await this.deleteIndex(
                            relatedCollection.getId(),
                            twoWayIndexKey,
                        );
                    } else {
                        await this.deleteIndex(col.getId(), indexKey);
                    }
                    break;
                case Database.RELATION_MANY_TO_ONE:
                    if (side === Database.RELATION_SIDE_PARENT) {
                        await this.deleteIndex(col.getId(), indexKey);
                    } else {
                        await this.deleteIndex(
                            relatedCollection.getId(),
                            twoWayIndexKey,
                        );
                    }
                    break;
                case Database.RELATION_MANY_TO_MANY:
                    const junction = this.getJunctionCollection(
                        col,
                        relatedCollection,
                        side,
                    );

                    await this.deleteDocument(Database.METADATA, junction);
                    break;
                default:
                    throw new RelationshipException(
                        "Invalid relationship type.",
                    );
            }
        });

        const deleted = await this.adapter.deleteRelationship(
            col.getId(),
            relatedCollection.getId(),
            type,
            twoWay,
            id,
            twoWayKey,
            side,
        );

        if (!deleted) {
            throw new DatabaseException("Failed to delete relationship");
        }

        await this.purgeCachedCollection(col.getId());
        await this.purgeCachedCollection(relatedCollection.getId());

        await this.trigger(Database.EVENT_ATTRIBUTE_DELETE, relationship);

        return true;
    }

    /**
     * Rename Index
     *
     * @param collection string
     * @param oldId string
     * @param newId string
     * @return Promise<boolean>
     * @throws AuthorizationException
     * @throws ConflictException
     * @throws DatabaseException
     * @throws DuplicateException
     * @throws StructureException
     */
    public async renameIndex(
        collection: string,
        oldId: string,
        newId: string,
    ): Promise<boolean> {
        const col = await this.silent(
            async () => await this.getCollection(collection),
        );

        const indexes = col.getAttribute("indexes", []);

        const indexExists = indexes.some(
            (index: any) => index.getId() === oldId,
        );

        if (!indexExists) {
            throw new NotFoundException("Index not found");
        }

        const newIndexExists = indexes.some(
            (index: any) => index.getId() === newId,
        );

        if (newIndexExists) {
            throw new DuplicateException("Index name already used");
        }

        let indexNew = null;
        for (const index of indexes) {
            if (index.getId() === oldId) {
                index.setAttribute("key", newId);
                index.setAttribute("$id", newId);
                indexNew = index;
                break;
            }
        }

        col.setAttribute("indexes", indexes);

        await this.adapter.renameIndex(col.getId(), oldId, newId);

        if (col.getId() !== Database.METADATA) {
            await this.silent(
                async () =>
                    await this.updateDocument(
                        Database.METADATA,
                        col.getId(),
                        col,
                    ),
            );
        }

        await this.trigger(Database.EVENT_INDEX_RENAME, indexNew);

        return true;
    }

    /**
     * Create Index
     *
     * @return Promise<boolean>
     * @throws AuthorizationException
     * @throws ConflictException
     * @throws DatabaseException
     * @throws DuplicateException
     * @throws LimitException
     * @throws StructureException
     * @throws Error
     */
    public async createIndex(params: CreateIndexParams): Promise<boolean>;
    public async createIndex(
        collection: string,
        id: string,
        type: string,
        attributes: string[],
        lengths?: number[],
        orders?: string[],
    ): Promise<boolean>;
    public async createIndex(
        collectionOrParams: string | CreateIndexParams,
        id?: string,
        type?: string,
        attributes: string[] = [],
        lengths: number[] = [],
        orders: string[] = [],
    ): Promise<boolean> {
        let collection: string;

        if (typeof collectionOrParams === "string") {
            collection = collectionOrParams;
        } else {
            collection = collectionOrParams.collection;
            id = collectionOrParams.id;
            type = collectionOrParams.type;
            attributes = collectionOrParams.attributes;
            lengths = collectionOrParams.lengths ?? [];
            orders = collectionOrParams.orders ?? [];
        }

        if (attributes.length === 0) {
            throw new DatabaseException("Missing attributes");
        }

        const col = await this.silent(
            async () => await this.getCollection(collection),
        );

        const indexes = col.getAttribute("indexes", []);

        for (const index of indexes) {
            if (index.getId().toLowerCase() === id!.toLowerCase()) {
                throw new DuplicateException("Index already exists");
            }
        }

        if (
            this.adapter.getCountOfIndexes(col) >=
            this.adapter.getLimitForIndexes()
        ) {
            throw new LimitException(
                "Index limit reached. Cannot create new index.",
            );
        }

        switch (type) {
            case Database.INDEX_KEY:
                if (!this.adapter.getSupportForIndex()) {
                    throw new DatabaseException("Key index is not supported");
                }
                break;

            case Database.INDEX_UNIQUE:
                if (!this.adapter.getSupportForUniqueIndex()) {
                    throw new DatabaseException(
                        "Unique index is not supported",
                    );
                }
                break;

            case Database.INDEX_FULLTEXT:
                if (!this.adapter.getSupportForFulltextIndex()) {
                    throw new DatabaseException(
                        "Fulltext index is not supported",
                    );
                }
                break;

            default:
                throw new DatabaseException(
                    `Unknown index type: ${type}. Must be one of ${Database.INDEX_KEY}, ${Database.INDEX_UNIQUE}, ${Database.INDEX_FULLTEXT}`,
                );
        }

        const collectionAttributes = col.getAttribute("attributes", []);

        for (let i = 0; i < attributes.length; i++) {
            const attr = attributes[i];
            for (const collectionAttribute of collectionAttributes) {
                if (collectionAttribute.getAttribute("key") === attr) {
                    if (
                        collectionAttribute.getAttribute("type") ===
                        Database.VAR_STRING
                    ) {
                        if (
                            lengths[i] &&
                            lengths[i] ===
                                collectionAttribute.getAttribute("size") &&
                            this.adapter.getMaxIndexLength() > 0
                        ) {
                            lengths[i] = null as any;
                        }
                    }

                    const isArray =
                        collectionAttribute.getAttribute("array") || false;
                    if (isArray) {
                        if (this.adapter.getMaxIndexLength() > 0) {
                            lengths[i] = Database.ARRAY_INDEX_LENGTH;
                        }
                        orders[i] = null as any;
                    }
                    break;
                }
            }
        }

        const index = new Document({
            $id: ID.custom(id!),
            key: id,
            type: type,
            attributes: attributes,
            lengths: lengths,
            orders: orders,
        });

        col.setAttribute("indexes", index, Document.SET_TYPE_APPEND);

        if (this.validate) {
            const validator = new IndexValidator(
                col.getAttribute("attributes", []),
                this.adapter.getMaxIndexLength(),
                this.adapter.getInternalIndexesKeys(),
            );
            if (!validator.isValid(index)) {
                throw new DatabaseException(validator.getDescription());
            }
        }

        try {
            const created = await this.adapter.createIndex(
                col.getId(),
                id!,
                type,
                attributes,
                lengths,
                orders,
            );

            if (!created) {
                throw new DatabaseException("Failed to create index");
            }
        } catch (e) {
            if (e instanceof DuplicateException) {
                if (!this.adapter.getSharedTables() || !this.isMigrating()) {
                    throw e;
                }
            } else {
                throw e;
            }
        }

        if (col.getId() !== Database.METADATA) {
            await this.silent(
                async () =>
                    await this.updateDocument(
                        Database.METADATA,
                        col.getId(),
                        col,
                    ),
            );
        }

        await this.trigger(Database.EVENT_INDEX_CREATE, index);

        return true;
    }

    /**
     * Delete Index
     *
     * @param collection string
     * @param id string
     *
     * @return Promise<boolean>
     * @throws AuthorizationException
     * @throws ConflictException
     * @throws DatabaseException
     * @throws StructureException
     */
    public async deleteIndex(
        collection: string | any,
        id: string,
    ): Promise<boolean> {
        collection = await this.silent(
            async () => await this.getCollection(collection),
        );

        let indexes = collection.getAttribute("indexes", []);

        let indexDeleted = null;
        for (const [key, value] of Object.entries(indexes)) {
            if ((value as any).getAttribute("$id") === id) {
                indexDeleted = value;
                delete indexes[key];
            }
        }

        const deleted = await this.adapter.deleteIndex(collection.getId(), id);

        collection.setAttribute("indexes", Object.values(indexes));

        if (collection.getId() !== Database.METADATA) {
            await this.silent(
                async () =>
                    await this.updateDocument(
                        Database.METADATA,
                        collection.getId(),
                        collection,
                    ),
            );
        }

        await this.trigger(Database.EVENT_INDEX_DELETE, indexDeleted);

        return deleted;
    }

    /**
     * Get Document
     *
     * @return Promise<Document>
     * @throws DatabaseException
     * @throws Exception
     */
    public async getDocument<T extends IRecord>(
        params: GetDocumentParams,
    ): Promise<Document<T>>;
    public async getDocument<T extends IRecord>(
        collection: string,
        id: string,
        queries?: Query[],
        forUpdate?: boolean,
    ): Promise<Document<T>>;
    public async getDocument<T extends IRecord>(
        collectionOrParams: string | GetDocumentParams,
        id?: string,
        queries: Query[] = [],
        forUpdate: boolean = false,
    ): Promise<Document<T>> {
        let collection: string;

        if (typeof collectionOrParams === "string") {
            collection = collectionOrParams;
        } else {
            collection = collectionOrParams.collection;
            id = collectionOrParams.id;
            queries = collectionOrParams.queries ?? [];
            forUpdate = collectionOrParams.forUpdate ?? false;
        }

        if (collection === Database.METADATA && id === Database.METADATA) {
            return new Document(Database.COLLECTION as any);
        }

        if (!collection) {
            throw new NotFoundException("Collection not found");
        }

        if (!id) {
            return new Document();
        }

        const _collection = await this.silent(
            async () => await this.getCollection<any>(collection),
        );

        if (_collection.isEmpty()) {
            throw new NotFoundException("Collection not found");
        }

        const attributes: any[] =
            _collection.getAttribute("attributes", []) ?? [];

        if (this.validate) {
            const validator = new DocumentValidator(attributes);
            if (!validator.isValid(queries)) {
                throw new QueryException(validator.getDescription());
            }
        }

        const relationships = attributes.filter(
            (attribute: Document) =>
                attribute.getAttribute("type") === Database.VAR_RELATIONSHIP,
        );

        const selects = Query.groupByType(queries)["selections"];
        const selections = this.validateSelections(_collection, selects);
        const nestedSelections: Query[] = [];

        for (const query of queries) {
            if (query.getMethod() === Query.TYPE_SELECT) {
                let values = query.getValues();
                for (
                    let valueIndex = 0;
                    valueIndex < values.length;
                    valueIndex++
                ) {
                    const value = values[valueIndex];
                    if (value.includes(".")) {
                        nestedSelections.push(
                            Query.select([value.split(".").slice(1).join(".")]),
                        );

                        const key = value.split(".")[0];

                        for (const relationship of relationships) {
                            if (relationship.getAttribute("key") === key) {
                                switch (
                                    relationship.getAttribute("options")[
                                        "relationType"
                                    ]
                                ) {
                                    case Database.RELATION_MANY_TO_MANY:
                                    case Database.RELATION_ONE_TO_MANY:
                                        values.splice(valueIndex, 1);
                                        break;

                                    case Database.RELATION_MANY_TO_ONE:
                                    case Database.RELATION_ONE_TO_ONE:
                                        values[valueIndex] = key;
                                        break;
                                }
                            }
                        }
                    }
                }
                query.setValues(values);
            }
        }

        queries = queries;

        const validator = new Authorization(Database.PERMISSION_READ);
        const documentSecurity = _collection.getAttribute(
            "documentSecurity",
            false,
        );

        const { collectionKey, documentKey, hashKey } = this.getCacheKeys(
            _collection.getId(),
            id,
            selections,
        );

        let cache: any = null;
        try {
            cache = await this.cache.load(
                documentKey!,
                this.cacheTTL,
                hashKey!,
            );
        } catch (e: any) {
            this.logger.warn(
                "Warning: Failed to get document from cache: " + e.message,
            );
            cache = null;
        }

        if (cache) {
            const document = new Document<any>(cache);

            if (_collection.getId() !== Database.METADATA) {
                if (
                    !validator.isValid([
                        ..._collection.getRead(),
                        ...(documentSecurity ? document.getRead() : []),
                    ])
                ) {
                    return new Document();
                }
            }

            await this.trigger(Database.EVENT_DOCUMENT_READ, document);

            return document;
        }

        let document = await this.adapter.getDocument(
            _collection.getId(),
            id,
            queries,
            forUpdate,
        );

        if (document.isEmpty()) {
            return document;
        }

        document.setAttribute("$collection", _collection.getId());

        if (_collection.getId() !== Database.METADATA) {
            if (
                !validator.isValid([
                    ..._collection.getRead(),
                    ...(documentSecurity ? document.getRead() : []),
                ])
            ) {
                return new Document();
            }
        }

        document = this.casting(_collection, document);
        document = await this.decode(_collection, document, selections);
        this.map = {};

        if (
            this.resolveRelationships &&
            (selects.length === 0 || nestedSelections.length > 0)
        ) {
            document = await this.silent(
                async () =>
                    await this.populateDocumentRelationships(
                        _collection,
                        document,
                        nestedSelections,
                    ),
            );
        }

        // Don't save to cache if it's part of a relationship
        if (relationships.length === 0) {
            try {
                await this.cache.save(
                    documentKey!,
                    document.toObject(),
                    hashKey!,
                );
                await this.cache.save(collectionKey, "empty", documentKey!);
            } catch (e: any) {
                this.logger.warn(
                    "Failed to save document to cache: " + e.message,
                );
            }
        }

        // for (const query of queries) {
        //     if (query.getMethod() === Query.TYPE_SELECT) {
        //         const values = query.getValues();
        //         for (const internalAttribute of this.getInternalAttributes()) {
        //             if (!values.includes(internalAttribute["$id"])) {
        //                 document.removeAttribute(internalAttribute["$id"]);
        //             }
        //         }
        //     }
        // }

        await this.trigger(Database.EVENT_DOCUMENT_READ, document);

        return document;
    }

    /**
     * @param collection Document
     * @param document Document
     * @param queries Query[]
     * @return Promise<Document>
     * @throws DatabaseException
     */
    private async populateDocumentRelationships(
        collection: Document,
        document: Document,
        queries: Query[] = [],
    ): Promise<Document> {
        const attributes = collection.getAttribute("attributes", []);

        const relationships = attributes.filter(
            (attribute: any) =>
                attribute.getAttribute("type") === Database.VAR_RELATIONSHIP,
        );

        for (const relationship of relationships) {
            const key = relationship.getAttribute("key");
            const value = document.getAttribute(key);
            const relatedCollection = await this.getCollection(
                relationship.getAttribute("options", {})["relatedCollection"],
            );
            const relationType = relationship.getAttribute("options", {})[
                "relationType"
            ];
            const twoWay = relationship.getAttribute("options", {})["twoWay"];
            const twoWayKey = relationship.getAttribute("options", {})[
                "twoWayKey"
            ];
            const side = relationship.getAttribute("options", {})["side"];

            if (value) {
                let k = `${relatedCollection.getId()}:${value}=>${collection.getId()}:${document.getId()}`;
                if (relationType === Database.RELATION_ONE_TO_MANY) {
                    k = `${collection.getId()}:${document.getId()}=>${relatedCollection.getId()}:${value}`;
                }
                this.map[k] = true;
            }

            relationship.setAttribute("collection", collection.getId());
            relationship.setAttribute("document", document.getId());

            let skipFetch = false;
            for (const fetchedRelationship of this.relationshipFetchStack) {
                const existingKey = fetchedRelationship.getAttribute("key");
                const existingCollection = fetchedRelationship.getCollection();
                const options = fetchedRelationship.getAttribute("options");
                const existingRelatedCollection = options["relatedCollection"];
                const existingTwoWayKey = options["twoWayKey"];
                const existingSide = options["side"];

                const reflexive = fetchedRelationship === relationship;

                const symmetric =
                    existingKey === twoWayKey &&
                    existingTwoWayKey === key &&
                    existingRelatedCollection === collection.getId() &&
                    existingCollection === relatedCollection.getId() &&
                    existingSide !== side;

                const transitive =
                    (existingKey === twoWayKey &&
                        existingCollection === relatedCollection.getId() &&
                        existingSide !== side) ||
                    (existingTwoWayKey === key &&
                        existingRelatedCollection === collection.getId() &&
                        existingSide !== side) ||
                    (existingKey === key &&
                        existingTwoWayKey !== twoWayKey &&
                        existingRelatedCollection ===
                            relatedCollection.getId() &&
                        existingSide !== side) ||
                    (existingKey !== key &&
                        existingTwoWayKey === twoWayKey &&
                        existingRelatedCollection ===
                            relatedCollection.getId() &&
                        existingSide !== side);

                if (reflexive || symmetric || transitive) {
                    skipFetch = true;
                }
            }

            switch (relationType) {
                case Database.RELATION_ONE_TO_ONE:
                    if (
                        skipFetch ||
                        (twoWay &&
                            this.relationshipFetchDepth ===
                                Database.RELATION_MAX_DEPTH)
                    ) {
                        document.removeAttribute(key);
                        break;
                    }

                    if (value === null) {
                        break;
                    }

                    this.relationshipFetchDepth++;
                    this.relationshipFetchStack.push(relationship);

                    const _related = await this.getDocument(
                        relatedCollection.getId(),
                        value,
                        queries,
                    );

                    this.relationshipFetchDepth--;
                    this.relationshipFetchStack.pop();

                    document.setAttribute(key, _related);
                    break;
                case Database.RELATION_ONE_TO_MANY:
                    if (side === Database.RELATION_SIDE_CHILD) {
                        if (
                            !twoWay ||
                            this.relationshipFetchDepth ===
                                Database.RELATION_MAX_DEPTH ||
                            skipFetch
                        ) {
                            document.removeAttribute(key);
                            break;
                        }
                        if (value !== null) {
                            this.relationshipFetchDepth++;
                            this.relationshipFetchStack.push(relationship);

                            const related = await this.getDocument(
                                relatedCollection.getId(),
                                value,
                                queries,
                            );

                            this.relationshipFetchDepth--;
                            this.relationshipFetchStack.pop();

                            document.setAttribute(key, related);
                        }
                        break;
                    }

                    if (
                        this.relationshipFetchDepth ===
                            Database.RELATION_MAX_DEPTH ||
                        skipFetch
                    ) {
                        break;
                    }

                    this.relationshipFetchDepth++;
                    this.relationshipFetchStack.push(relationship);

                    const _relatedDocuments = await this.find(
                        relatedCollection.getId(),
                        [
                            Query.equal(twoWayKey, [document.getId()]),
                            Query.limit(Number.MAX_SAFE_INTEGER),
                            ...queries,
                        ],
                    );

                    this.relationshipFetchDepth--;
                    this.relationshipFetchStack.pop();

                    for (const related of _relatedDocuments) {
                        related.removeAttribute(twoWayKey);
                    }

                    document.setAttribute(key, _relatedDocuments);
                    break;
                case Database.RELATION_MANY_TO_ONE:
                    if (side === Database.RELATION_SIDE_PARENT) {
                        if (
                            skipFetch ||
                            this.relationshipFetchDepth ===
                                Database.RELATION_MAX_DEPTH
                        ) {
                            document.removeAttribute(key);
                            break;
                        }

                        if (value === null) {
                            break;
                        }
                        this.relationshipFetchDepth++;
                        this.relationshipFetchStack.push(relationship);

                        const related = await this.getDocument(
                            relatedCollection.getId(),
                            value,
                            queries,
                        );

                        this.relationshipFetchDepth--;
                        this.relationshipFetchStack.pop();

                        document.setAttribute(key, related);
                        break;
                    }

                    if (!twoWay) {
                        document.removeAttribute(key);
                        break;
                    }

                    if (
                        this.relationshipFetchDepth ===
                            Database.RELATION_MAX_DEPTH ||
                        skipFetch
                    ) {
                        break;
                    }

                    this.relationshipFetchDepth++;
                    this.relationshipFetchStack.push(relationship);

                    const relatedDocuments = await this.find(
                        relatedCollection.getId(),
                        [
                            Query.equal(twoWayKey, [document.getId()]),
                            Query.limit(Number.MAX_SAFE_INTEGER),
                            ...queries,
                        ],
                    );

                    this.relationshipFetchDepth--;
                    this.relationshipFetchStack.pop();

                    for (const related of relatedDocuments) {
                        related.removeAttribute(twoWayKey);
                    }

                    document.setAttribute(key, relatedDocuments);
                    break;
                case Database.RELATION_MANY_TO_MANY:
                    if (!twoWay && side === Database.RELATION_SIDE_CHILD) {
                        break;
                    }

                    if (
                        twoWay &&
                        (this.relationshipFetchDepth ===
                            Database.RELATION_MAX_DEPTH ||
                            skipFetch)
                    ) {
                        break;
                    }

                    this.relationshipFetchDepth++;
                    this.relationshipFetchStack.push(relationship);

                    const junction = this.getJunctionCollection(
                        collection,
                        relatedCollection,
                        side,
                    );

                    const junctions = await this.skipRelationships(
                        async () =>
                            await this.find(junction, [
                                Query.equal(twoWayKey, [document.getId()]),
                                Query.limit(Number.MAX_SAFE_INTEGER),
                            ]),
                    );

                    const related = [];
                    for (const junction of junctions) {
                        related.push(
                            await this.getDocument(
                                relatedCollection.getId(),
                                junction.getAttribute(key),
                                queries,
                            ),
                        );
                    }

                    this.relationshipFetchDepth--;
                    this.relationshipFetchStack.pop();

                    document.setAttribute(key, related);
                    break;
            }
        }
        return document;
    }

    /**
     * Create Document
     *
     * @param collection
     * @param document
     *
     * @return Document
     *
     * @throws AuthorizationException
     * @throws DatabaseException
     * @throws StructureException
     */
    public async createDocument<T extends IRecord>(
        collection: string,
        document: Document<T>,
    ): Promise<Document<T>> {
        if (
            collection !== Database.METADATA &&
            this.adapter.getSharedTables() &&
            !this.adapter.getTenantId()
        ) {
            throw new DatabaseException(
                "Missing tenant. Tenant must be set when table sharing is enabled.",
            );
        }

        const _collection = await this.silent(
            async () => await this.getCollection(collection),
        );

        if (_collection.getId() !== Database.METADATA) {
            const authorization = new Authorization(Database.PERMISSION_CREATE);
            if (!authorization.isValid(_collection.getCreate())) {
                throw new AuthorizationException(
                    authorization.getDescription(),
                );
            }
        }

        const time = DateTime.now();

        const createdAt = document.getCreatedAt();
        const updatedAt = document.getUpdatedAt();

        document
            .setAttribute(
                "$id",
                document.getId() ? document.getId() : ID.unique(),
            )
            .setAttribute("$collection", _collection.getId())
            .setAttribute(
                "$createdAt",
                createdAt && this.preserveDates ? createdAt : time,
            )
            .setAttribute(
                "$updatedAt",
                updatedAt && this.preserveDates ? updatedAt : time,
            );

        if (this.adapter.getSharedTables()) {
            document.setAttribute(
                "$tenant",
                String(this.adapter.getTenantId()),
            );
        }

        document = await this.encode(_collection, document);

        if (this.validate) {
            const validator = new Permissions();
            if (!validator.isValid(document.getPermissions())) {
                throw new DatabaseException(validator.getDescription());
            }
        }

        const structure = new Structure(
            _collection,
            this.adapter.getMinDateTime(),
            this.adapter.getMaxDateTime(),
        );
        if (!structure.isValid(document)) {
            throw new StructureException(structure.getDescription());
        }

        document = await this.withTransaction(async () => {
            if (this.resolveRelationships) {
                document = await this.silent(
                    async () =>
                        await this.createDocumentRelationships(
                            _collection,
                            document,
                        ),
                );
            }

            return await this.adapter.createDocument(
                _collection.getId(),
                document,
            );
        });

        if (this.resolveRelationships) {
            document = await this.silent(
                async () =>
                    await this.populateDocumentRelationships(
                        _collection,
                        document,
                    ),
            );
        }

        document = await this.decode(_collection, document);

        await this.trigger(Database.EVENT_DOCUMENT_CREATE, document);

        return document;
    }

    /**
     * Create Documents in a batch
     *
     * @param collection
     * @param documents
     * @param batchSize
     *
     * @return array<Document>
     *
     * @throws AuthorizationException
     * @throws StructureException
     * @throws Exception
     */
    public async createDocuments(
        collection: string,
        documents: Document[],
        batchSize: number = Database.INSERT_BATCH_SIZE,
    ): Promise<Document[]> {
        if (documents.length === 0) {
            return [];
        }

        const _collection = await this.silent(
            async () => await this.getCollection(collection),
        );

        const time = DateTime.now();

        for (let i = 0; i < documents.length; i++) {
            let document = documents[i]!;
            const createdAt = document.getCreatedAt();
            const updatedAt = document.getUpdatedAt();

            document
                .setAttribute(
                    "$id",
                    document.getId() ? document.getId() : ID.unique(),
                )
                .setAttribute("$collection", _collection.getId())
                .setAttribute(
                    "$createdAt",
                    createdAt && this.preserveDates ? createdAt : time,
                )
                .setAttribute(
                    "$updatedAt",
                    updatedAt && this.preserveDates ? updatedAt : time,
                );

            document = await this.encode(_collection, document);

            const validator = new Structure(
                _collection,
                this.adapter.getMinDateTime(),
                this.adapter.getMaxDateTime(),
            );
            if (!validator.isValid(document)) {
                throw new StructureException(validator.getDescription());
            }

            if (this.resolveRelationships) {
                documents[i] = await this.silent(
                    async () =>
                        await this.createDocumentRelationships(
                            _collection,
                            document,
                        ),
                );
            } else {
                documents[i] = document;
            }
        }

        documents = await this.withTransaction(async () => {
            return await this.adapter.createDocuments(
                _collection.getId(),
                documents,
                batchSize,
            );
        });

        for (let i = 0; i < documents.length; i++) {
            let document = documents[i]!;
            if (this.resolveRelationships) {
                document = await this.silent(
                    async () =>
                        await this.populateDocumentRelationships(
                            _collection,
                            document,
                        ),
                );
            }
            documents[i] = await this.decode(_collection, document);
        }

        await this.trigger(
            Database.EVENT_DOCUMENTS_CREATE,
            new Document({
                $collection: _collection.getId(),
                modified: documents.length,
            }),
        );

        return documents;
    }

    /**
     * @param collection
     * @param document
     * @return Document
     * @throws DatabaseException
     */
    private async createDocumentRelationships(
        collection: Document,
        document: Document,
    ): Promise<Document> {
        const attributes = collection.getAttribute("attributes", []);

        const relationships = attributes.filter(
            (attribute: any) =>
                attribute.getAttribute("type") === Database.VAR_RELATIONSHIP,
        );

        const stackCount = this.relationshipWriteStack.length;

        for (const relationship of relationships) {
            const key = relationship.getAttribute("key");
            const value = document.getAttribute(key) ?? undefined;
            const options = relationship.getAttribute("options", {});
            const relatedCollection = await this.getCollection(
                options.relatedCollection,
            );
            const relationType = options.relationType;
            const twoWay = options.twoWay;
            const twoWayKey = options.twoWayKey;
            const side = options.side;

            if (
                stackCount >= Database.RELATION_MAX_DEPTH - 1 &&
                this.relationshipWriteStack[stackCount - 1] !==
                    relatedCollection.getId()
            ) {
                document.removeAttribute(key);
                continue;
            }

            this.relationshipWriteStack.push(collection.getId());

            try {
                switch (typeof value) {
                    case "object":
                        if (value === null) {
                            if (
                                (relationType ===
                                    Database.RELATION_ONE_TO_MANY &&
                                    side === Database.RELATION_SIDE_CHILD) ||
                                (relationType ===
                                    Database.RELATION_MANY_TO_ONE &&
                                    side === Database.RELATION_SIDE_PARENT) ||
                                (relationType ===
                                    Database.RELATION_ONE_TO_ONE &&
                                    side === Database.RELATION_SIDE_PARENT) ||
                                (relationType ===
                                    Database.RELATION_ONE_TO_ONE &&
                                    side === Database.RELATION_SIDE_CHILD &&
                                    twoWay)
                            ) {
                                break;
                            }

                            document.removeAttribute(key);
                            // No related document
                            break;
                        }
                        if (Array.isArray(value)) {
                            if (
                                (relationType ===
                                    Database.RELATION_MANY_TO_ONE &&
                                    side === Database.RELATION_SIDE_PARENT) ||
                                (relationType ===
                                    Database.RELATION_ONE_TO_MANY &&
                                    side === Database.RELATION_SIDE_CHILD) ||
                                relationType === Database.RELATION_ONE_TO_ONE
                            ) {
                                throw new RelationshipException(
                                    "Invalid relationship value. Must be either a document ID or a document, array given.",
                                );
                            }

                            // List of documents or IDs
                            for (const related of value) {
                                switch (typeof related) {
                                    case "object":
                                        if (!(related instanceof Document)) {
                                            throw new RelationshipException(
                                                "Invalid relationship value. Must be either a document, document ID, or an array of documents or document IDs.",
                                            );
                                        }
                                        await this.relateDocuments(
                                            collection,
                                            relatedCollection,
                                            key,
                                            document,
                                            related,
                                            relationType,
                                            twoWay,
                                            twoWayKey,
                                            side,
                                        );
                                        break;
                                    case "string":
                                        await this.relateDocumentsById(
                                            collection,
                                            relatedCollection,
                                            key,
                                            document.getId(),
                                            related,
                                            relationType,
                                            twoWay,
                                            twoWayKey,
                                            side,
                                        );
                                        break;
                                    default:
                                        throw new RelationshipException(
                                            "Invalid relationship value. Must be either a document, document ID, or an array of documents or document IDs.",
                                        );
                                }
                            }
                            document.removeAttribute(key);
                        } else {
                            if (!(value instanceof Document)) {
                                throw new RelationshipException(
                                    "Invalid relationship value. Must be either a document, document ID, or an array of documents or document IDs.",
                                );
                            }

                            if (
                                relationType === Database.RELATION_ONE_TO_ONE &&
                                !twoWay &&
                                side === Database.RELATION_SIDE_CHILD
                            ) {
                                throw new RelationshipException(
                                    "Invalid relationship value. Cannot set a value from the child side of a oneToOne relationship when twoWay is false.",
                                );
                            }

                            if (
                                (relationType ===
                                    Database.RELATION_ONE_TO_MANY &&
                                    side === Database.RELATION_SIDE_PARENT) ||
                                (relationType ===
                                    Database.RELATION_MANY_TO_ONE &&
                                    side === Database.RELATION_SIDE_CHILD) ||
                                relationType === Database.RELATION_MANY_TO_MANY
                            ) {
                                throw new RelationshipException(
                                    "Invalid relationship value. Must be either an array of documents or document IDs, document given.",
                                );
                            }

                            const relatedId = await this.relateDocuments(
                                collection,
                                relatedCollection,
                                key,
                                document,
                                value,
                                relationType,
                                twoWay,
                                twoWayKey,
                                side,
                            );
                            document.setAttribute(key, relatedId);
                        }
                        break;

                    case "string":
                        if (
                            relationType === Database.RELATION_ONE_TO_ONE &&
                            !twoWay &&
                            side === Database.RELATION_SIDE_CHILD
                        ) {
                            throw new RelationshipException(
                                "Invalid relationship value. Cannot set a value from the child side of a oneToOne relationship when twoWay is false.",
                            );
                        }

                        if (
                            (relationType === Database.RELATION_ONE_TO_MANY &&
                                side === Database.RELATION_SIDE_PARENT) ||
                            (relationType === Database.RELATION_MANY_TO_ONE &&
                                side === Database.RELATION_SIDE_CHILD) ||
                            relationType === Database.RELATION_MANY_TO_MANY
                        ) {
                            throw new RelationshipException(
                                "Invalid relationship value. Must be either an array of documents or document IDs, document ID given.",
                            );
                        }

                        // Single document ID
                        await this.relateDocumentsById(
                            collection,
                            relatedCollection,
                            key,
                            document.getId(),
                            value,
                            relationType,
                            twoWay,
                            twoWayKey,
                            side,
                        );
                        break;

                    case "undefined":
                        // TODO: This might need to depend on the relation type, to be either set to null or removed?

                        if (
                            (relationType === Database.RELATION_ONE_TO_MANY &&
                                side === Database.RELATION_SIDE_CHILD) ||
                            (relationType === Database.RELATION_MANY_TO_ONE &&
                                side === Database.RELATION_SIDE_PARENT) ||
                            (relationType === Database.RELATION_ONE_TO_ONE &&
                                side === Database.RELATION_SIDE_PARENT) ||
                            (relationType === Database.RELATION_ONE_TO_ONE &&
                                side === Database.RELATION_SIDE_CHILD &&
                                twoWay)
                        ) {
                            break;
                        }

                        document.removeAttribute(key);
                        // No related document
                        break;

                    default:
                        throw new RelationshipException(
                            "Invalid relationship value. Must be either a document, document ID, or an array of documents or document IDs.",
                        );
                }
            } finally {
                this.relationshipWriteStack.pop();
            }
        }

        return document;
    }

    /**
     * @param collection
     * @param relatedCollection
     * @param key
     * @param document
     * @param relation
     * @param relationType
     * @param twoWay
     * @param twoWayKey
     * @param side
     * @return string related document ID
     *
     * @throws AuthorizationException
     * @throws ConflictException
     * @throws StructureException
     * @throws Exception
     */
    private async relateDocuments(
        collection: Document,
        relatedCollection: Document,
        key: string,
        document: Document,
        relation: Document,
        relationType: string,
        twoWay: boolean,
        twoWayKey: string,
        side: string,
    ): Promise<string> {
        switch (relationType) {
            case Database.RELATION_ONE_TO_ONE:
                if (twoWay) {
                    relation.setAttribute(twoWayKey, document.getId());
                }
                break;
            case Database.RELATION_ONE_TO_MANY:
                if (side === Database.RELATION_SIDE_PARENT) {
                    relation.setAttribute(twoWayKey, document.getId());
                }
                break;
            case Database.RELATION_MANY_TO_ONE:
                if (side === Database.RELATION_SIDE_CHILD) {
                    relation.setAttribute(twoWayKey, document.getId());
                }
                break;
        }

        // Try to get the related document
        let related = await this.getDocument(
            relatedCollection.getId(),
            relation.getId(),
        );

        if (related.isEmpty()) {
            // If the related document doesn't exist, create it, inheriting permissions if none are set
            if (!relation.getAttribute("$permissions")) {
                relation.setAttribute(
                    "$permissions",
                    document.getPermissions(),
                );
            }

            related = await this.createDocument(
                relatedCollection.getId(),
                relation,
            );
        } else if (related.getAttributes() !== relation.getAttributes()) {
            // If the related document exists and the data is not the same, update it
            for (const [attribute, value] of Object.entries(
                relation.getAttributes(),
            )) {
                related.setAttribute(attribute, value);
            }

            related = await this.updateDocument(
                relatedCollection.getId(),
                related.getId(),
                related,
            );
        }

        if (relationType === Database.RELATION_MANY_TO_MANY) {
            const junction = this.getJunctionCollection(
                collection,
                relatedCollection,
                side,
            );

            await this.createDocument(
                junction,
                new Document({
                    [key]: related.getId(),
                    [twoWayKey]: document.getId(),
                    $permissions: [
                        Permission.read(Role.any()),
                        Permission.update(Role.any()),
                        Permission.delete(Role.any()),
                    ],
                }),
            );
        }

        return related.getId();
    }

    /**
     * @param collection
     * @param relatedCollection
     * @param key
     * @param documentId
     * @param relationId
     * @param relationType
     * @param twoWay
     * @param twoWayKey
     * @param side
     * @return void
     * @throws AuthorizationException
     * @throws ConflictException
     * @throws StructureException
     * @throws Exception
     */
    private async relateDocumentsById(
        collection: Document,
        relatedCollection: Document,
        key: string,
        documentId: string,
        relationId: string,
        relationType: string,
        twoWay: boolean,
        twoWayKey: string,
        side: string,
    ): Promise<void> {
        // Get the related document, will be empty on permissions failure
        let related = await this.skipRelationships(
            async () =>
                await this.getDocument(relatedCollection.getId(), relationId),
        );

        if (related.isEmpty() && this.checkRelationshipsExist) {
            return;
        }

        switch (relationType) {
            case Database.RELATION_ONE_TO_ONE:
                if (twoWay) {
                    related.setAttribute(twoWayKey, documentId);
                    await this.skipRelationships(
                        async () =>
                            await this.updateDocument(
                                relatedCollection.getId(),
                                relationId,
                                related,
                            ),
                    );
                }
                break;
            case Database.RELATION_ONE_TO_MANY:
                if (side === Database.RELATION_SIDE_PARENT) {
                    related.setAttribute(twoWayKey, documentId);
                    await this.skipRelationships(
                        async () =>
                            await this.updateDocument(
                                relatedCollection.getId(),
                                relationId,
                                related,
                            ),
                    );
                }
                break;
            case Database.RELATION_MANY_TO_ONE:
                if (side === Database.RELATION_SIDE_CHILD) {
                    related.setAttribute(twoWayKey, documentId);
                    await this.skipRelationships(
                        async () =>
                            await this.updateDocument(
                                relatedCollection.getId(),
                                relationId,
                                related,
                            ),
                    );
                }
                break;
            case Database.RELATION_MANY_TO_MANY:
                await this.purgeCachedDocument(
                    relatedCollection.getId(),
                    relationId,
                );

                const junction = this.getJunctionCollection(
                    collection,
                    relatedCollection,
                    side,
                );

                await this.skipRelationships(
                    async () =>
                        await this.createDocument(
                            junction,
                            new Document({
                                [key]: relationId,
                                [twoWayKey]: documentId,
                                $permissions: [
                                    Permission.read(Role.any()),
                                    Permission.update(Role.any()),
                                    Permission.delete(Role.any()),
                                ],
                            }),
                        ),
                );
                break;
        }
    }

    /**
     * Update Document
     *
     * @param collection - The collection name
     * @param id - The document ID
     * @param document - The document to update
     * @returns The updated document
     *
     * @throws AuthorizationException
     * @throws ConflictException
     * @throws DatabaseException
     * @throws StructureException
     */
    public async updateDocument<T extends IRecord>(
        collection: string,
        id: string,
        document: Document,
    ): Promise<Document<T>> {
        if (!id) {
            throw new DatabaseException("Must define id attribute");
        }

        const _collection = await this.silent(
            async () => await this.getCollection(collection),
        );

        document = await this.withTransaction(async () => {
            const time = new Date();
            const old = await Authorization.skip(
                async () =>
                    await this.silent(
                        async () =>
                            await this.getDocument(
                                _collection.getId(),
                                id,
                                undefined,
                                true,
                            ),
                    ),
            );

            const _document = {
                ...old.toObject(),
                ...document.toObject(),
            };
            _document["$collection"] = old.getCollection(); // Make sure user doesn't switch collection ID
            _document["$createdAt"] = old.getCreatedAt(); // Make sure user doesn't switch createdAt

            if (this.adapter.getSharedTables()) {
                _document["$tenant"] = old.getAttribute("$tenant"); // Make sure user doesn't switch tenant
            }

            document = new Document(_document);

            const relationships = _collection
                .getAttribute("attributes", [])
                .filter((attribute: any) => {
                    return (
                        attribute.getAttribute("type") ===
                        Database.VAR_RELATIONSHIP
                    );
                });

            const updateValidator = new Authorization(
                Database.PERMISSION_UPDATE,
            );
            const readValidator = new Authorization(Database.PERMISSION_READ);
            let shouldUpdate = false;

            if (_collection.getId() !== Database.METADATA) {
                const documentSecurity = _collection.getAttribute(
                    "documentSecurity",
                    false,
                );

                for (const relationship of relationships) {
                    relationships[relationship.getAttribute("key")] =
                        relationship;
                }

                // Compare if the document has any changes
                for (const [key, value] of (document as any).entries()) {
                    // Skip the nested documents as they will be checked later in recursions.
                    if (relationships.hasOwnProperty(key)) {
                        // No need to compare nested documents more than max depth.
                        if (
                            this.relationshipWriteStack.length >=
                            Database.RELATION_MAX_DEPTH - 1
                        ) {
                            continue;
                        }
                        const relationType =
                            relationships[key].getAttribute(
                                "options",
                            ).relationType;
                        const side =
                            relationships[key].getAttribute("options").side;
                        switch (relationType) {
                            case Database.RELATION_ONE_TO_ONE:
                                const oldValue =
                                    old.getAttribute(key) instanceof Document
                                        ? old.getAttribute(key).getId()
                                        : old.getAttribute(key);

                                if (
                                    (value === null) !== (oldValue === null) ||
                                    (typeof value === "string" &&
                                        value !== oldValue) ||
                                    (value instanceof Document &&
                                        value.getId() !== oldValue)
                                ) {
                                    shouldUpdate = true;
                                }
                                break;
                            case Database.RELATION_ONE_TO_MANY:
                            case Database.RELATION_MANY_TO_ONE:
                            case Database.RELATION_MANY_TO_MANY:
                                if (
                                    (relationType ===
                                        Database.RELATION_MANY_TO_ONE &&
                                        side ===
                                            Database.RELATION_SIDE_PARENT) ||
                                    (relationType ===
                                        Database.RELATION_ONE_TO_MANY &&
                                        side === Database.RELATION_SIDE_CHILD)
                                ) {
                                    const oldValue =
                                        old.getAttribute(key) instanceof
                                        Document
                                            ? old.getAttribute(key).getId()
                                            : old.getAttribute(key);

                                    if (
                                        (value === null) !==
                                            (oldValue === null) ||
                                        (typeof value === "string" &&
                                            value !== oldValue) ||
                                        (value instanceof Document &&
                                            value.getId() !== oldValue)
                                    ) {
                                        shouldUpdate = true;
                                    }
                                    break;
                                }

                                if (!Array.isArray(value)) {
                                    throw new RelationshipException(
                                        "Invalid relationship value. Must be either an array of documents or document IDs, " +
                                            typeof value +
                                            " given.",
                                    );
                                }

                                if (
                                    old.getAttribute(key).length !==
                                    value.length
                                ) {
                                    shouldUpdate = true;
                                    break;
                                }

                                for (const [
                                    index,
                                    relation,
                                ] of value.entries()) {
                                    const oldValue =
                                        old.getAttribute(key)[index] instanceof
                                        Document
                                            ? old
                                                  .getAttribute(key)
                                                  [index].getId()
                                            : old.getAttribute(key)[index];

                                    if (
                                        (typeof relation === "string" &&
                                            relation !== oldValue) ||
                                        (relation instanceof Document &&
                                            relation.getId() !== oldValue)
                                    ) {
                                        shouldUpdate = true;
                                        break;
                                    }
                                }
                                break;
                        }

                        if (shouldUpdate) {
                            break;
                        }

                        continue;
                    }

                    const oldValue = old.getAttribute(key);

                    // If values are not equal we need to update document.
                    if (value !== oldValue) {
                        shouldUpdate = true;
                        break;
                    }
                }

                const updatePermissions = [
                    ..._collection.getUpdate(),
                    ...(documentSecurity ? old.getUpdate() : []),
                ];

                const readPermissions = [
                    ..._collection.getRead(),
                    ...(documentSecurity ? old.getRead() : []),
                ];

                if (
                    shouldUpdate &&
                    !updateValidator.isValid(updatePermissions)
                ) {
                    throw new AuthorizationException(
                        updateValidator.getDescription(),
                    );
                } else if (
                    !shouldUpdate &&
                    !readValidator.isValid(readPermissions)
                ) {
                    throw new AuthorizationException(
                        readValidator.getDescription(),
                    );
                }
            }

            if (old.isEmpty()) {
                return new Document();
            }

            if (shouldUpdate) {
                const updatedAt = document.getUpdatedAt();
                document.setAttribute(
                    "$updatedAt",
                    !updatedAt || !this.preserveDates ? time : updatedAt,
                );
            }

            // Check if document was updated after the request timestamp
            const oldUpdatedAt = new Date(old.getUpdatedAt() as string);
            if (this.timestamp && oldUpdatedAt > this.timestamp) {
                throw new ConflictException(
                    "Document was updated after the request timestamp",
                );
            }

            document = await this.encode(_collection, document);

            const structureValidator = new Structure(
                _collection,
                this.adapter.getMinDateTime(),
                this.adapter.getMaxDateTime(),
            );
            if (!structureValidator.isValid(document)) {
                // Make sure updated structure still apply collection rules (if any)
                throw new StructureException(
                    structureValidator.getDescription(),
                );
            }

            if (this.resolveRelationships) {
                document = await this.silent(
                    async () =>
                        await this.updateDocumentRelationships(
                            _collection,
                            old,
                            document,
                        ),
                );
            }

            await this.adapter.updateDocument(
                _collection.getId(),
                id,
                document,
            );

            return document;
        });

        if (this.resolveRelationships) {
            document = await this.silent(
                async () =>
                    await this.populateDocumentRelationships(
                        _collection,
                        document,
                    ),
            );
        }

        document = await this.decode(_collection, document);

        await this.purgeRelatedDocuments(_collection, id);
        await this.purgeCachedDocument(_collection.getId(), id);
        await this.trigger(Database.EVENT_DOCUMENT_UPDATE, document);

        return document;
    }

    /**
     * Update documents
     *
     * Updates all documents which match the given query.
     *
     * @param collection - The collection name
     * @param updates - The updates to apply
     * @param queries - The queries to match documents
     * @param batchSize - The batch size for updates
     * @returns The updated documents
     *
     * @throws AuthorizationException
     * @throws DatabaseException
     */
    public async updateDocuments(
        collection: string,
        updates: Document,
        queries: Query[] = [],
        batchSize: number = Database.INSERT_BATCH_SIZE,
    ): Promise<Document[]> {
        if (updates.isEmpty()) {
            return [];
        }

        const _collection = await this.silent(
            async () => await this.getCollection(collection),
        );

        if (_collection.isEmpty()) {
            throw new DatabaseException("Collection not found");
        }

        const attributes = _collection.getAttribute("attributes", []);
        const indexes = _collection.getAttribute("indexes", []);

        if (this.validate) {
            const validator = new DocumentsValidator(
                attributes,
                indexes,
                this.maxQueryValues,
                this.adapter.getMinDateTime(),
                this.adapter.getMaxDateTime(),
            );

            if (!validator.isValid(queries)) {
                throw new QueryException(validator.getDescription());
            }
        }

        const grouped = Query.groupByType(queries);
        let limit = grouped["limit"];
        const cursor = grouped["cursor"];

        if (cursor && cursor.getCollection() !== _collection.getId()) {
            throw new DatabaseException(
                "cursor Document must be from the same Collection.",
            );
        }

        updates.removeAttribute("$id");
        updates.removeAttribute("$createdAt");
        updates.removeAttribute("$tenant");

        if (!this.preserveDates) {
            updates.setAttribute("$updatedAt", new Date());
        }

        updates = await this.encode(_collection, updates);

        // Check new document structure
        const validator = new PartialStructure(
            _collection,
            this.adapter.getMinDateTime(),
            this.adapter.getMaxDateTime(),
        );

        if (!validator.isValid(updates)) {
            throw new StructureException(validator.getDescription());
        }

        const documents = await this.withTransaction(async () => {
            let lastDocument: any = null;
            const documents: Document[] = [];

            const documentSecurity = _collection.getAttribute(
                "documentSecurity",
                false,
            );

            const authorization = new Authorization(Database.PERMISSION_UPDATE);
            const skipAuth = authorization.isValid(_collection.getUpdate());

            if (
                !skipAuth &&
                !documentSecurity &&
                _collection.getId() !== Database.METADATA
            ) {
                throw new AuthorizationException(
                    authorization.getDescription(),
                );
            }

            const originalLimit = limit;
            lastDocument = cursor;

            // Resolve and update relationships
            while (true) {
                if (limit && limit < batchSize) {
                    batchSize = limit;
                } else if (limit) {
                    limit -= batchSize;
                }

                const affectedDocuments = await this.silent(
                    async () =>
                        await this.find(
                            _collection.getId(),
                            [
                                ...queries,
                                ...(lastDocument
                                    ? [
                                          Query.limit(batchSize),
                                          Query.cursorAfter(lastDocument),
                                      ]
                                    : [Query.limit(batchSize)]),
                            ],
                            Database.PERMISSION_UPDATE,
                        ),
                );

                if (affectedDocuments.length === 0) {
                    break;
                }

                for (const document of affectedDocuments) {
                    if (this.resolveRelationships) {
                        const newDocument = new Document({
                            ...document.toObject(),
                            ...updates.toObject(),
                        });
                        await this.silent(
                            async () =>
                                await this.updateDocumentRelationships(
                                    _collection,
                                    document,
                                    newDocument,
                                ),
                        );
                        documents.push(newDocument);
                    }

                    // Check if document was updated after the request timestamp
                    const oldUpdatedAt = new Date(
                        document.getUpdatedAt() as string,
                    );
                    if (this.timestamp && oldUpdatedAt > this.timestamp) {
                        throw new ConflictException(
                            "Document was updated after the request timestamp",
                        );
                    }
                }

                const getResults = async () =>
                    await this.adapter.updateDocuments(
                        _collection.getId(),
                        updates,
                        affectedDocuments,
                    );

                skipAuth
                    ? await Authorization.skip(getResults)
                    : await getResults();

                if (affectedDocuments.length < batchSize) {
                    break;
                } else if (
                    originalLimit &&
                    documents.length === originalLimit
                ) {
                    break;
                }

                lastDocument = affectedDocuments[affectedDocuments.length - 1];
            }

            for (const document of documents) {
                await this.purgeRelatedDocuments(_collection, document.getId());
                await this.purgeCachedDocument(
                    _collection.getId(),
                    document.getId(),
                );
            }

            await this.trigger(
                Database.EVENT_DOCUMENTS_UPDATE,
                new Document({
                    $collection: _collection.getId(),
                    modified: documents.length,
                }),
            );

            return documents;
        });

        return documents;
    }

    /**
     * @param collection - The collection document
     * @param old - The old document
     * @param document - The new document
     * @returns The updated document
     * @throws AuthorizationException
     * @throws ConflictException
     * @throws DatabaseException
     * @throws DuplicateException
     * @throws StructureException
     */
    private async updateDocumentRelationships(
        collection: Document,
        old: Document,
        document: Document,
    ): Promise<Document> {
        const attributes = collection.getAttribute("attributes", []);

        const relationships = attributes.filter((attribute: any) => {
            return attribute.getAttribute("type") === Database.VAR_RELATIONSHIP;
        });

        const stackCount = this.relationshipWriteStack.length;

        for (const relationship of relationships) {
            const key = relationship.getAttribute("key");
            const value = document.getAttribute(key);
            const oldValue = old.getAttribute(key);
            const options = relationship.getAttribute("options", {});
            const relatedCollection = await this.getCollection(
                options.relatedCollection,
            );
            const relationType = options.relationType as string;
            const twoWay = options.twoWay as boolean;
            const twoWayKey = options.twoWayKey as string;
            const side = options.side as string;

            if (oldValue === value) {
                if (
                    (relationType === Database.RELATION_ONE_TO_ONE ||
                        (relationType === Database.RELATION_MANY_TO_ONE &&
                            side === Database.RELATION_SIDE_PARENT)) &&
                    value instanceof Document
                ) {
                    document.setAttribute(key, value.getId());
                    continue;
                }
                document.removeAttribute(key);
                continue;
            }

            if (
                stackCount >= Database.RELATION_MAX_DEPTH - 1 &&
                this.relationshipWriteStack[stackCount - 1] !==
                    relatedCollection.getId()
            ) {
                document.removeAttribute(key);
                continue;
            }

            this.relationshipWriteStack.push(collection.getId());

            try {
                switch (relationType) {
                    case Database.RELATION_ONE_TO_ONE:
                        if (!twoWay) {
                            if (side === Database.RELATION_SIDE_CHILD) {
                                throw new RelationshipException(
                                    "Invalid relationship value. Cannot set a value from the child side of a oneToOne relationship when twoWay is false.",
                                );
                            }

                            if (typeof value === "string") {
                                const related = await this.skipRelationships(
                                    async () =>
                                        await this.getDocument(
                                            relatedCollection.getId(),
                                            value,
                                            [Query.select(["$id"])],
                                        ),
                                );
                                if (related.isEmpty()) {
                                    // If no such document exists in related collection
                                    // For one-one we need to update the related key to null if no relation exists
                                    document.setAttribute(key, null);
                                }
                            } else if (value instanceof Document) {
                                const relationId = await this.relateDocuments(
                                    collection,
                                    relatedCollection,
                                    key,
                                    document,
                                    value,
                                    relationType,
                                    false,
                                    twoWayKey,
                                    side,
                                );
                                document.setAttribute(key, relationId);
                            } else if (Array.isArray(value)) {
                                throw new RelationshipException(
                                    "Invalid relationship value. Must be either a document, document ID or null. Array given.",
                                );
                            }

                            break;
                        }

                        switch (typeof value) {
                            case "string":
                                const related = await this.skipRelationships(
                                    async () =>
                                        await this.getDocument(
                                            relatedCollection.getId(),
                                            value,
                                            [Query.select(["$id"])],
                                        ),
                                );

                                if (related.isEmpty()) {
                                    // If no such document exists in related collection
                                    // For one-one we need to update the related key to null if no relation exists
                                    document.setAttribute(key, null);
                                    break;
                                }
                                if (
                                    oldValue?.getId() !== value &&
                                    !(
                                        await this.skipRelationships(
                                            async () =>
                                                await this.findOne(
                                                    relatedCollection.getId(),
                                                    [
                                                        Query.select(["$id"]),
                                                        Query.equal(twoWayKey, [
                                                            value,
                                                        ]),
                                                    ],
                                                ),
                                        )
                                    ).isEmpty()
                                ) {
                                    // Have to do this here because otherwise relations would be updated before the database can throw the unique violation
                                    throw new DuplicateException(
                                        "Document already has a related document",
                                    );
                                }

                                await this.skipRelationships(
                                    async () =>
                                        await this.updateDocument(
                                            relatedCollection.getId(),
                                            related.getId(),
                                            related.setAttribute(
                                                twoWayKey,
                                                document.getId(),
                                            ),
                                        ),
                                );
                                break;
                            // @ts-ignore
                            case "object":
                                if (value instanceof Document) {
                                    let related = await this.skipRelationships(
                                        async () =>
                                            await this.getDocument(
                                                relatedCollection.getId(),
                                                value.getId(),
                                            ),
                                    );

                                    if (
                                        oldValue?.getId() !== value.getId() &&
                                        !(
                                            await this.skipRelationships(
                                                async () =>
                                                    await this.findOne(
                                                        relatedCollection.getId(),
                                                        [
                                                            Query.select([
                                                                "$id",
                                                            ]),
                                                            Query.equal(
                                                                twoWayKey,
                                                                [value.getId()],
                                                            ),
                                                        ],
                                                    ),
                                            )
                                        ).isEmpty()
                                    ) {
                                        // Have to do this here because otherwise relations would be updated before the database can throw the unique violation
                                        throw new DuplicateException(
                                            "Document already has a related document",
                                        );
                                    }

                                    this.relationshipWriteStack.push(
                                        relatedCollection.getId(),
                                    );
                                    if (related.isEmpty()) {
                                        if (
                                            !value.getAttribute(
                                                "$permissions",
                                                false,
                                            )
                                        ) {
                                            value.setAttribute(
                                                "$permissions",
                                                document.getAttribute(
                                                    "$permissions",
                                                ),
                                            );
                                        }
                                        related = await this.createDocument(
                                            relatedCollection.getId(),
                                            value.setAttribute(
                                                twoWayKey,
                                                document.getId(),
                                            ),
                                        );
                                    } else {
                                        related = await this.updateDocument(
                                            relatedCollection.getId(),
                                            related.getId(),
                                            value.setAttribute(
                                                twoWayKey,
                                                document.getId(),
                                            ),
                                        );
                                    }
                                    this.relationshipWriteStack.pop();

                                    document.setAttribute(key, related.getId());
                                    break;
                                }
                            // no break
                            case "null" as any:
                                if (oldValue?.getId()) {
                                    const oldRelated =
                                        await this.skipRelationships(
                                            async () =>
                                                await this.getDocument(
                                                    relatedCollection.getId(),
                                                    oldValue.getId(),
                                                ),
                                        );
                                    await this.skipRelationships(
                                        async () =>
                                            await this.updateDocument(
                                                relatedCollection.getId(),
                                                oldRelated.getId(),
                                                oldRelated.setAttribute(
                                                    twoWayKey,
                                                    null,
                                                ),
                                            ),
                                    );
                                }
                                break;
                            default:
                                throw new RelationshipException(
                                    "Invalid relationship value. Must be either a document, document ID or null.",
                                );
                        }
                        break;
                    case Database.RELATION_ONE_TO_MANY:
                    case Database.RELATION_MANY_TO_ONE:
                        if (
                            (relationType === Database.RELATION_ONE_TO_MANY &&
                                side === Database.RELATION_SIDE_PARENT) ||
                            (relationType === Database.RELATION_MANY_TO_ONE &&
                                side === Database.RELATION_SIDE_CHILD)
                        ) {
                            if (
                                !Array.isArray(value) ||
                                !Array.isArray(value)
                            ) {
                                throw new RelationshipException(
                                    "Invalid relationship value. Must be either an array of documents or document IDs, " +
                                        typeof value +
                                        " given.",
                                );
                            }

                            const oldIds = oldValue.map((doc: Document) =>
                                doc.getId(),
                            );

                            const newIds = value.map((item: any) => {
                                if (typeof item === "string") {
                                    return item;
                                } else if (item instanceof Document) {
                                    return item.getId();
                                } else {
                                    throw new RelationshipException(
                                        "Invalid relationship value. No ID provided.",
                                    );
                                }
                            });

                            const removedDocuments = oldIds.filter(
                                (id: string) => !newIds.includes(id),
                            );

                            for (const relation of removedDocuments) {
                                await Authorization.skip(
                                    async () =>
                                        await this.skipRelationships(
                                            async () =>
                                                await this.updateDocument(
                                                    relatedCollection.getId(),
                                                    relation,
                                                    new Document({
                                                        [twoWayKey]: null,
                                                    }),
                                                ),
                                        ),
                                );
                            }

                            for (const relation of value) {
                                if (typeof relation === "string") {
                                    const related =
                                        await this.skipRelationships(
                                            async () =>
                                                await this.getDocument(
                                                    relatedCollection.getId(),
                                                    relation,
                                                    [Query.select(["$id"])],
                                                ),
                                        );

                                    if (related.isEmpty()) {
                                        continue;
                                    }

                                    await this.skipRelationships(
                                        async () =>
                                            await this.updateDocument(
                                                relatedCollection.getId(),
                                                related.getId(),
                                                related.setAttribute(
                                                    twoWayKey,
                                                    document.getId(),
                                                ),
                                            ),
                                    );
                                } else if (relation instanceof Document) {
                                    const related =
                                        await this.skipRelationships(
                                            async () =>
                                                await this.getDocument(
                                                    relatedCollection.getId(),
                                                    relation.getId(),
                                                    [Query.select(["$id"])],
                                                ),
                                        );

                                    if (related.isEmpty()) {
                                        if (
                                            !relation.getAttribute(
                                                "$permissions",
                                                false,
                                            )
                                        ) {
                                            relation.setAttribute(
                                                "$permissions",
                                                document.getAttribute(
                                                    "$permissions",
                                                ),
                                            );
                                        }
                                        await this.createDocument(
                                            relatedCollection.getId(),
                                            relation.setAttribute(
                                                twoWayKey,
                                                document.getId(),
                                            ),
                                        );
                                    } else {
                                        await this.updateDocument(
                                            relatedCollection.getId(),
                                            related.getId(),
                                            relation.setAttribute(
                                                twoWayKey,
                                                document.getId(),
                                            ),
                                        );
                                    }
                                } else {
                                    throw new RelationshipException(
                                        "Invalid relationship value.",
                                    );
                                }
                            }

                            document.removeAttribute(key);
                            break;
                        }

                        if (typeof value === "string") {
                            const related = await this.skipRelationships(
                                async () =>
                                    await this.getDocument(
                                        relatedCollection.getId(),
                                        value,
                                        [Query.select(["$id"])],
                                    ),
                            );

                            if (related.isEmpty()) {
                                // If no such document exists in related collection
                                // For many-one we need to update the related key to null if no relation exists
                                document.setAttribute(key, null);
                            }
                            await this.purgeCachedDocument(
                                relatedCollection.getId(),
                                value,
                            );
                        } else if (value instanceof Document) {
                            const related = await this.skipRelationships(
                                async () =>
                                    await this.getDocument(
                                        relatedCollection.getId(),
                                        value.getId(),
                                        [Query.select(["$id"])],
                                    ),
                            );

                            if (related.isEmpty()) {
                                if (
                                    !value.getAttribute("$permissions", false)
                                ) {
                                    value.setAttribute(
                                        "$permissions",
                                        document.getAttribute("$permissions"),
                                    );
                                }
                                await this.createDocument(
                                    relatedCollection.getId(),
                                    value,
                                );
                            } else if (
                                related.getAttributes() !==
                                value.getAttributes()
                            ) {
                                await this.updateDocument(
                                    relatedCollection.getId(),
                                    related.getId(),
                                    value,
                                );
                                await this.purgeCachedDocument(
                                    relatedCollection.getId(),
                                    related.getId(),
                                );
                            }

                            document.setAttribute(key, value.getId());
                        } else if (value === null) {
                            break;
                        } else if (Array.isArray(value)) {
                            throw new RelationshipException(
                                "Invalid relationship value. Must be either a document ID or a document, array given.",
                            );
                        } else if (value === undefined) {
                            throw new RelationshipException(
                                "Invalid relationship value. Must be either a document ID or a document.",
                            );
                        } else {
                            throw new RelationshipException(
                                "Invalid relationship value.",
                            );
                        }

                        break;
                    case Database.RELATION_MANY_TO_MANY:
                        if (value === null) {
                            break;
                        }
                        if (!Array.isArray(value)) {
                            throw new RelationshipException(
                                "Invalid relationship value. Must be an array of documents or document IDs.",
                            );
                        }

                        const oldIds = oldValue.map((doc: Document) =>
                            doc.getId(),
                        );

                        const newIds = value.map((item: any) => {
                            if (typeof item === "string") {
                                return item;
                            } else if (item instanceof Document) {
                                return item.getId();
                            } else {
                                throw new RelationshipException(
                                    "Invalid relationship value. Must be either a document or document ID.",
                                );
                            }
                        });

                        const removedDocuments = oldIds.filter(
                            (id: string) => !newIds.includes(id),
                        );

                        for (const relation of removedDocuments) {
                            const junction = this.getJunctionCollection(
                                collection,
                                relatedCollection,
                                side,
                            );

                            const junctions = await this.find(junction, [
                                Query.equal(key, [relation]),
                                Query.equal(twoWayKey, [document.getId()]),
                                Query.limit(Number.MAX_SAFE_INTEGER),
                            ]);

                            for (const junction of junctions) {
                                await Authorization.skip(
                                    async () =>
                                        await this.deleteDocument(
                                            junction.getCollection(),
                                            junction.getId(),
                                        ),
                                );
                            }
                        }

                        for (let relation of value) {
                            if (typeof relation === "string") {
                                if (
                                    oldIds.includes(relation) ||
                                    (
                                        await this.getDocument(
                                            relatedCollection.getId(),
                                            relation,
                                            [Query.select(["$id"])],
                                        )
                                    ).isEmpty()
                                ) {
                                    continue;
                                }
                            } else if (relation instanceof Document) {
                                let related = await this.getDocument(
                                    relatedCollection.getId(),
                                    relation.getId(),
                                    [Query.select(["$id"])],
                                );

                                if (related.isEmpty()) {
                                    if (
                                        !relation.getAttribute(
                                            "$permissions",
                                            false,
                                        )
                                    ) {
                                        relation.setAttribute(
                                            "$permissions",
                                            document.getAttribute(
                                                "$permissions",
                                            ),
                                        );
                                    }
                                    related = await this.createDocument(
                                        relatedCollection.getId(),
                                        relation,
                                    );
                                } else if (
                                    related.getAttributes() !==
                                    relation.getAttributes()
                                ) {
                                    related = await this.updateDocument(
                                        relatedCollection.getId(),
                                        related.getId(),
                                        relation,
                                    );
                                }

                                if (oldIds.includes(relation.getId())) {
                                    continue;
                                }

                                relation = related.getId();
                            } else {
                                throw new RelationshipException(
                                    "Invalid relationship value. Must be either a document or document ID.",
                                );
                            }

                            await this.skipRelationships(
                                async () =>
                                    await this.createDocument(
                                        this.getJunctionCollection(
                                            collection,
                                            relatedCollection,
                                            side,
                                        ),
                                        new Document({
                                            [key]: relation,
                                            [twoWayKey]: document.getId(),
                                            $permissions: [
                                                Permission.read(Role.any()),
                                                Permission.update(Role.any()),
                                                Permission.delete(Role.any()),
                                            ],
                                        }),
                                    ),
                            );
                        }

                        document.removeAttribute(key);
                        break;
                }
            } finally {
                this.relationshipWriteStack.pop();
            }
        }

        return document;
    }

    private getJunctionCollection(
        collection: Document,
        relatedCollection: Document,
        side: string,
    ): string {
        return side === Database.RELATION_SIDE_PARENT
            ? `_${collection.getInternalId()}_${relatedCollection.getInternalId()}`
            : `_${relatedCollection.getInternalId()}_${collection.getInternalId()}`;
    }

    /**
     * Increase a document attribute by a value
     *
     * @returns boolean
     *
     * @throws AuthorizationException
     * @throws DatabaseException
     * @throws Exception
     */
    public async increaseDocumentAttribute(
        params: IncreaseDocumentAttributeParams,
    ): Promise<boolean>;
    public async increaseDocumentAttribute(
        collection: string,
        id: string,
        attribute: string,
        value?: number,
        max?: number | null,
    ): Promise<boolean>;
    public async increaseDocumentAttribute(
        collectionOrParams: string | IncreaseDocumentAttributeParams,
        id?: string,
        attribute?: string,
        value: number = 1,
        max: number | null = null,
    ): Promise<boolean> {
        let collection: string;

        if (typeof collectionOrParams === "string") {
            collection = collectionOrParams;
        } else {
            collection = collectionOrParams.collection;
            id = collectionOrParams.id;
            attribute = collectionOrParams.attribute;
            value = collectionOrParams.value ?? 1;
            max = collectionOrParams.max ?? null;
        }

        if (value <= 0) {
            // Can be a float
            throw new DatabaseException(
                "Value must be numeric and greater than 0",
            );
        }

        const validator = new Authorization(Database.PERMISSION_UPDATE);

        const document = await Authorization.skip(
            async () =>
                await this.silent(
                    async () => await this.getDocument(collection, id!),
                ),
        ); // Skip ensures user does not need read permission for this

        if (document.isEmpty()) {
            return false;
        }

        const _collection = await this.silent(
            async () => await this.getCollection(collection),
        );

        if (_collection.getId() !== Database.METADATA) {
            const documentSecurity = _collection.getAttribute(
                "documentSecurity",
                false,
            );
            if (
                !validator.isValid([
                    ..._collection.getUpdate(),
                    ...(documentSecurity ? document.getUpdate() : []),
                ])
            ) {
                throw new AuthorizationException(validator.getDescription());
            }
        }

        const attr = _collection
            .getAttribute("attributes", [])
            .filter((a: any) => a.getId() === attribute);

        if (attr.length === 0) {
            throw new NotFoundException("Attribute not found");
        }

        const whiteList = [Database.VAR_INTEGER, Database.VAR_FLOAT];

        const attributeObj = attr[attr.length - 1];
        if (!whiteList.includes(attributeObj.getAttribute("type"))) {
            throw new DatabaseException(
                "Attribute type must be one of: " + whiteList.join(","),
            );
        }

        if (max && document.getAttribute(attribute!) + value > max) {
            throw new DatabaseException(
                "Attribute value exceeds maximum limit: " + max,
            );
        }

        const time = DateTime.now();
        let updatedAt = document.getUpdatedAt();
        updatedAt =
            updatedAt === null || !this.preserveDates ? time : updatedAt;

        const oldUpdatedAt = new Date(document.getUpdatedAt() as string);
        if (this.timestamp !== null && oldUpdatedAt > this.timestamp) {
            throw new ConflictException(
                "Document was updated after the request timestamp",
            );
        }

        max = max ? max - value : null;

        const result = await this.adapter.increaseDocumentAttribute(
            _collection.getId(),
            id!,
            attribute!,
            value,
            updatedAt as string,
            undefined,
            max as number,
        );

        await this.purgeCachedDocument(_collection.getId(), id!);

        await this.trigger(Database.EVENT_DOCUMENT_INCREASE, document);

        return result;
    }

    /**
     * Decrease a document attribute by a value
     *
     * @returns boolean
     *
     * @throws AuthorizationException
     * @throws DatabaseException
     */
    public async decreaseDocumentAttribute(
        params: DecreaseDocumentAttributeParams,
    ): Promise<boolean>;
    public async decreaseDocumentAttribute(
        collection: string,
        id: string,
        attribute: string,
        value?: number,
        min?: number | null,
    ): Promise<boolean>;
    public async decreaseDocumentAttribute(
        collectionOrParams: string | DecreaseDocumentAttributeParams,
        id?: string,
        attribute?: string,
        value: number = 1,
        min: number | null = null,
    ): Promise<boolean> {
        let collection: string;

        if (typeof collectionOrParams === "string") {
            collection = collectionOrParams;
        } else {
            collection = collectionOrParams.collection;
            id = collectionOrParams.id;
            attribute = collectionOrParams.attribute;
            value = collectionOrParams.value ?? 1;
            min = collectionOrParams.min ?? null;
        }

        if (value <= 0) {
            // Can be a float
            throw new DatabaseException(
                "Value must be numeric and greater than 0",
            );
        }

        const validator = new Authorization(Database.PERMISSION_UPDATE);

        const document = await Authorization.skip(
            async () =>
                await this.silent(
                    async () => await this.getDocument(collection, id!),
                ),
        ); // Skip ensures user does not need read permission for this

        if (document.isEmpty()) {
            return false;
        }

        const _collection = await this.silent(
            async () => await this.getCollection(collection),
        );

        if (_collection.getId() !== Database.METADATA) {
            const documentSecurity = _collection.getAttribute(
                "documentSecurity",
                false,
            );
            if (
                !validator.isValid([
                    ..._collection.getUpdate(),
                    ...(documentSecurity ? document.getUpdate() : []),
                ])
            ) {
                throw new AuthorizationException(validator.getDescription());
            }
        }

        const attr = _collection
            .getAttribute("attributes", [])
            .filter((a: any) => a.getId() === attribute);

        if (attr.length === 0) {
            throw new NotFoundException("Attribute not found");
        }

        const whiteList = [Database.VAR_INTEGER, Database.VAR_FLOAT];

        const attributeObj = attr[attr.length - 1];
        if (!whiteList.includes(attributeObj.getAttribute("type"))) {
            throw new DatabaseException(
                "Attribute type must be one of: " + whiteList.join(","),
            );
        }

        if (min && document.getAttribute(attribute!) - value < min) {
            throw new DatabaseException(
                "Attribute value exceeds minimum limit: " + min,
            );
        }

        const time = DateTime.now();
        let updatedAt = document.getUpdatedAt();
        updatedAt =
            updatedAt === null || !this.preserveDates ? time : updatedAt;

        const oldUpdatedAt = new Date(document.getUpdatedAt() as string);
        if (this.timestamp !== null && oldUpdatedAt > this.timestamp) {
            throw new ConflictException(
                "Document was updated after the request timestamp",
            );
        }

        min = min ? min + value : null;

        const result = await this.adapter.increaseDocumentAttribute(
            _collection.getId(),
            id!,
            attribute!,
            value * -1,
            updatedAt as string,
            min as number,
        );

        await this.purgeCachedDocument(_collection.getId(), id!);

        await this.trigger(Database.EVENT_DOCUMENT_DECREASE, document);

        return result;
    }

    /**
     * Delete Document
     *
     * @param collection - The collection name
     * @param id - The document ID
     * @returns boolean
     *
     * @throws AuthorizationException
     * @throws ConflictException
     * @throws DatabaseException
     * @throws RestrictedException
     */
    public async deleteDocument(
        collection: string,
        id: string,
    ): Promise<boolean> {
        const _collection = await this.silent(
            async () => await this.getCollection(collection),
        );

        let _document;

        const deleted = await this.withTransaction(async () => {
            let document = await Authorization.skip(
                async () =>
                    await this.silent(
                        async () =>
                            await this.getDocument(
                                _collection.getId(),
                                id,
                                undefined,
                                true,
                            ),
                    ),
            );

            if (document.isEmpty()) {
                return false;
            }

            const validator = new Authorization(Database.PERMISSION_DELETE);

            if (_collection.getId() !== Database.METADATA) {
                const documentSecurity = _collection.getAttribute(
                    "documentSecurity",
                    false,
                );
                if (
                    !validator.isValid([
                        ..._collection.getDelete(),
                        ...(documentSecurity ? document.getDelete() : []),
                    ])
                ) {
                    throw new AuthorizationException(
                        validator.getDescription(),
                    );
                }
            }

            // Check if document was updated after the request timestamp
            const oldUpdatedAt = new Date(document.getUpdatedAt() as string);
            if (this.timestamp !== null && oldUpdatedAt > this.timestamp) {
                throw new ConflictException(
                    "Document was updated after the request timestamp",
                );
            }

            if (this.resolveRelationships) {
                document = await this.silent(
                    async () =>
                        await this.deleteDocumentRelationships(
                            _collection,
                            document,
                        ),
                );
            }

            _document = document;
            return await this.adapter.deleteDocument(_collection.getId(), id);
        });

        await this.purgeRelatedDocuments(_collection, id);
        await this.purgeCachedDocument(_collection.getId(), id);

        await this.trigger(Database.EVENT_DOCUMENT_DELETE, _document);

        return deleted;
    }

    /**
     * @param collection - The collection document
     * @param document - The document to delete relationships for
     * @returns Document
     * @throws AuthorizationException
     * @throws ConflictException
     * @throws DatabaseException
     * @throws RestrictedException
     * @throws StructureException
     */
    private async deleteDocumentRelationships(
        collection: Document,
        document: Document,
    ): Promise<Document> {
        const attributes = collection.getAttribute("attributes", []);

        const relationships = attributes.filter(
            (attribute: any) =>
                attribute.getAttribute("type") === Database.VAR_RELATIONSHIP,
        );

        for (const relationship of relationships) {
            const key = relationship.getAttribute("key");
            const value = document.getAttribute(key);
            const options = relationship.getAttribute("options", {});
            const relatedCollection = await this.getCollection(
                options["relatedCollection"],
            );
            const relationType = options["relationType"];
            const twoWay = options["twoWay"];
            const twoWayKey = options["twoWayKey"];
            const onDelete = options["onDelete"];
            const side = options["side"];

            relationship.setAttribute("collection", collection.getId());
            relationship.setAttribute("document", document.getId());

            switch (onDelete) {
                case Database.RELATION_MUTATE_RESTRICT:
                    await this.deleteRestrict(
                        relatedCollection,
                        document,
                        value,
                        relationType,
                        twoWay,
                        twoWayKey,
                        side,
                    );
                    break;
                case Database.RELATION_MUTATE_SET_NULL:
                    await this.deleteSetNull(
                        collection,
                        relatedCollection,
                        document,
                        value,
                        relationType,
                        twoWay,
                        twoWayKey,
                        side,
                    );
                    break;
                case Database.RELATION_MUTATE_CASCADE:
                    for (const processedRelationship of this
                        .relationshipDeleteStack) {
                        const existingKey =
                            processedRelationship.getAttribute("key");
                        const existingCollection =
                            processedRelationship.getAttribute("collection");
                        const options = processedRelationship.getAttribute(
                            "options",
                            {},
                        );
                        const existingRelatedCollection =
                            options["relatedCollection"];
                        const existingTwoWayKey = options["twoWayKey"];
                        const existingSide = options["side"];

                        const reflexive =
                            processedRelationship === relationship;

                        const symmetric =
                            existingKey === twoWayKey &&
                            existingTwoWayKey === key &&
                            existingRelatedCollection === collection.getId() &&
                            existingCollection === relatedCollection.getId() &&
                            existingSide !== side;

                        const transitive =
                            (existingKey === twoWayKey &&
                                existingCollection ===
                                    relatedCollection.getId() &&
                                existingSide !== side) ||
                            (existingTwoWayKey === key &&
                                existingRelatedCollection ===
                                    collection.getId() &&
                                existingSide !== side) ||
                            (existingKey === key &&
                                existingTwoWayKey !== twoWayKey &&
                                existingRelatedCollection ===
                                    relatedCollection.getId() &&
                                existingSide !== side) ||
                            (existingKey !== key &&
                                existingTwoWayKey === twoWayKey &&
                                existingRelatedCollection ===
                                    relatedCollection.getId() &&
                                existingSide !== side);

                        if (reflexive || symmetric || transitive) {
                            break;
                        }
                    }
                    await this.deleteCascade(
                        collection,
                        relatedCollection,
                        document,
                        key,
                        value,
                        relationType,
                        twoWayKey,
                        side,
                        relationship,
                    );
                    break;
            }
        }

        return document;
    }

    private async deleteRestrict(
        relatedCollection: Document,
        document: Document,
        value: any,
        relationType: string,
        twoWay: boolean,
        twoWayKey: string,
        side: string,
    ): Promise<void> {
        if (value instanceof Document && value.isEmpty()) {
            value = null;
        }

        if (
            value &&
            relationType !== Database.RELATION_MANY_TO_ONE &&
            side === Database.RELATION_SIDE_PARENT
        ) {
            throw new RestrictedException(
                "Cannot delete document because it has at least one related document.",
            );
        }

        if (
            relationType === Database.RELATION_ONE_TO_ONE &&
            side === Database.RELATION_SIDE_CHILD &&
            !twoWay
        ) {
            await Authorization.skip(async () => {
                const related = await this.findOne(relatedCollection.getId(), [
                    Query.select(["$id"]),
                    Query.equal(twoWayKey, [document.getId()]),
                ]);

                if (related.isEmpty()) {
                    return;
                }

                await this.skipRelationships(
                    async () =>
                        await this.updateDocument(
                            relatedCollection.getId(),
                            related.getId(),
                            new Document({
                                [twoWayKey]: null,
                            }),
                        ),
                );
            });
        }

        if (
            relationType === Database.RELATION_MANY_TO_ONE &&
            side === Database.RELATION_SIDE_CHILD
        ) {
            const related = await Authorization.skip(
                async () =>
                    await this.findOne(relatedCollection.getId(), [
                        Query.select(["$id"]),
                        Query.equal(twoWayKey, [document.getId()]),
                    ]),
            );

            if (!related.isEmpty()) {
                throw new RestrictedException(
                    "Cannot delete document because it has at least one related document.",
                );
            }
        }
    }

    private async deleteSetNull(
        collection: Document,
        relatedCollection: Document,
        document: Document,
        value: any,
        relationType: string,
        twoWay: boolean,
        twoWayKey: string,
        side: string,
    ): Promise<void> {
        switch (relationType) {
            case Database.RELATION_ONE_TO_ONE:
                if (!twoWay && side === Database.RELATION_SIDE_PARENT) {
                    break;
                }

                await Authorization.skip(async () => {
                    let related;
                    if (!twoWay && side === Database.RELATION_SIDE_CHILD) {
                        related = await this.findOne(
                            relatedCollection.getId(),
                            [
                                Query.select(["$id"]),
                                Query.equal(twoWayKey, [document.getId()]),
                            ],
                        );
                    } else {
                        if (!value) {
                            return;
                        }
                        related = await this.getDocument(
                            relatedCollection.getId(),
                            value.getId(),
                            [Query.select(["$id"])],
                        );
                    }

                    if (related.isEmpty()) {
                        return;
                    }

                    await this.skipRelationships(
                        async () =>
                            await this.updateDocument(
                                relatedCollection.getId(),
                                related.getId(),
                                new Document({
                                    [twoWayKey]: null,
                                }),
                            ),
                    );
                });
                break;

            case Database.RELATION_ONE_TO_MANY:
                if (side === Database.RELATION_SIDE_CHILD) {
                    break;
                }
                for (const relation of value) {
                    await Authorization.skip(async () => {
                        await this.skipRelationships(
                            async () =>
                                await this.updateDocument(
                                    relatedCollection.getId(),
                                    relation.getId(),
                                    new Document({
                                        [twoWayKey]: null,
                                    }),
                                ),
                        );
                    });
                }
                break;

            case Database.RELATION_MANY_TO_ONE:
                if (side === Database.RELATION_SIDE_PARENT) {
                    break;
                }

                if (!twoWay) {
                    value = await this.find(relatedCollection.getId(), [
                        Query.select(["$id"]),
                        Query.equal(twoWayKey, [document.getId()]),
                        Query.limit(Number.MAX_SAFE_INTEGER),
                    ]);
                }

                for (const relation of value) {
                    await Authorization.skip(async () => {
                        await this.skipRelationships(
                            async () =>
                                await this.updateDocument(
                                    relatedCollection.getId(),
                                    relation.getId(),
                                    new Document({
                                        [twoWayKey]: null,
                                    }),
                                ),
                        );
                    });
                }
                break;

            case Database.RELATION_MANY_TO_MANY:
                const junction = this.getJunctionCollection(
                    collection,
                    relatedCollection,
                    side,
                );

                const junctions = await this.find(junction, [
                    Query.select(["$id"]),
                    Query.equal(twoWayKey, [document.getId()]),
                    Query.limit(Number.MAX_SAFE_INTEGER),
                ]);

                for (const doc of junctions) {
                    await this.skipRelationships(
                        async () =>
                            await this.deleteDocument(junction, doc.getId()),
                    );
                }
                break;
        }
    }

    private async deleteCascade(
        collection: Document,
        relatedCollection: Document,
        document: Document,
        key: string,
        value: any,
        relationType: string,
        twoWayKey: string,
        side: string,
        relationship: Document,
    ): Promise<void> {
        switch (relationType) {
            case Database.RELATION_ONE_TO_ONE:
                if (value !== null) {
                    this.relationshipDeleteStack.push(relationship);

                    await this.deleteDocument(
                        relatedCollection.getId(),
                        value instanceof Document ? value.getId() : value,
                    );

                    this.relationshipDeleteStack.pop();
                }
                break;
            case Database.RELATION_ONE_TO_MANY:
                if (side === Database.RELATION_SIDE_CHILD) {
                    break;
                }

                this.relationshipDeleteStack.push(relationship);

                for (const relation of value) {
                    await this.deleteDocument(
                        relatedCollection.getId(),
                        relation.getId(),
                    );
                }

                this.relationshipDeleteStack.pop();
                break;
            case Database.RELATION_MANY_TO_ONE:
                if (side === Database.RELATION_SIDE_PARENT) {
                    break;
                }

                value = await this.find(relatedCollection.getId(), [
                    Query.select(["$id"]),
                    Query.equal(twoWayKey, [document.getId()]),
                    Query.limit(Number.MAX_SAFE_INTEGER),
                ]);

                this.relationshipDeleteStack.push(relationship);

                for (const relation of value) {
                    await this.deleteDocument(
                        relatedCollection.getId(),
                        relation.getId(),
                    );
                }

                this.relationshipDeleteStack.pop();
                break;
            case Database.RELATION_MANY_TO_MANY:
                const junction = this.getJunctionCollection(
                    collection,
                    relatedCollection,
                    side,
                );

                const junctions = await this.skipRelationships(
                    async () =>
                        await this.find(junction, [
                            Query.select(["$id", key]),
                            Query.equal(twoWayKey, [document.getId()]),
                            Query.limit(Number.MAX_SAFE_INTEGER),
                        ]),
                );

                this.relationshipDeleteStack.push(relationship);

                for (const doc of junctions) {
                    if (side === Database.RELATION_SIDE_PARENT) {
                        await this.deleteDocument(
                            relatedCollection.getId(),
                            doc.getAttribute(key),
                        );
                    }
                    await this.deleteDocument(junction, doc.getId());
                }

                this.relationshipDeleteStack.pop();
                break;
        }
    }

    /**
     * Delete Documents
     *
     * Deletes all documents which match the given query, will respect the relationship's onDelete option.
     *
     * @param collection string
     * @param queries Query[]
     * @param batchSize number
     *
     * @return Promise<Document[]>
     *
     * @throws AuthorizationException
     * @throws DatabaseException
     * @throws RestrictedException
     */
    public async deleteDocuments(
        collection: string,
        queries: Query[] = [],
        batchSize: number = Database.DELETE_BATCH_SIZE,
    ): Promise<Document[]> {
        if (this.adapter.getSharedTables() && !this.adapter.getTenantId()) {
            throw new DatabaseException(
                "Missing tenant. Tenant must be set when table sharing is enabled.",
            );
        }

        const _collection = await this.silent(
            async () => await this.getCollection(collection),
        );

        if (!_collection || _collection.isEmpty()) {
            throw new DatabaseException("Collection not found");
        }

        const attributes = _collection.getAttribute("attributes", []);
        const indexes = _collection.getAttribute("indexes", []);

        if (this.validate) {
            const validator = new DocumentsValidator(
                attributes,
                indexes,
                this.maxQueryValues,
                this.adapter.getMinDateTime(),
                this.adapter.getMaxDateTime(),
            );

            if (!validator.isValid(queries)) {
                throw new QueryException(validator.getDescription());
            }
        }

        const grouped = Query.groupByType(queries);
        let limit = grouped["limit"];
        const cursor = grouped["cursor"];

        if (cursor && cursor.getCollection() !== _collection.getId()) {
            throw new DatabaseException(
                "cursor Document must be from the same Collection.",
            );
        }

        const documents = await this.withTransaction(async () => {
            const documentSecurity = _collection.getAttribute(
                "documentSecurity",
                false,
            );
            const authorization = new Authorization(Database.PERMISSION_DELETE);
            const skipAuth = authorization.isValid(_collection.getDelete());
            let documents: Document[] = [];

            if (
                !skipAuth &&
                !documentSecurity &&
                _collection.getId() !== Database.METADATA
            ) {
                throw new AuthorizationException(
                    authorization.getDescription(),
                );
            }

            const originalLimit = limit;
            let lastDocument = cursor;

            while (true) {
                if (limit && limit < batchSize) {
                    batchSize = limit;
                } else if (limit) {
                    limit -= batchSize;
                }

                const affectedDocuments = await this.silent(
                    async () =>
                        await this.find(
                            _collection.getId(),
                            [
                                ...queries,
                                ...(lastDocument
                                    ? [
                                          Query.limit(batchSize),
                                          Query.cursorAfter(lastDocument),
                                      ]
                                    : [Query.limit(batchSize)]),
                            ],
                            Database.PERMISSION_DELETE,
                        ),
                );

                if (!affectedDocuments.length) {
                    break;
                }

                documents = [...affectedDocuments, ...documents];

                for (const document of affectedDocuments) {
                    // Delete Relationships
                    if (this.resolveRelationships) {
                        await this.silent(
                            async () =>
                                await this.deleteDocumentRelationships(
                                    _collection,
                                    document,
                                ),
                        );
                    }

                    // Check if document was updated after the request timestamp
                    const oldUpdatedAt = new Date(
                        document.getUpdatedAt() as string,
                    );

                    if (this.timestamp && oldUpdatedAt > this.timestamp) {
                        throw new ConflictException(
                            "Document was updated after the request timestamp",
                        );
                    }

                    await this.purgeRelatedDocuments(
                        _collection,
                        document.getId(),
                    );
                    await this.purgeCachedDocument(
                        _collection.getId(),
                        document.getId(),
                    );
                }

                if (affectedDocuments.length < batchSize) {
                    break;
                } else if (
                    originalLimit &&
                    documents.length === originalLimit
                ) {
                    break;
                }

                lastDocument = affectedDocuments[affectedDocuments.length - 1];
            }

            if (!documents.length) {
                return [];
            }

            await this.trigger(
                Database.EVENT_DOCUMENTS_DELETE,
                new Document({
                    $collection: _collection.getId(),
                    modified: documents.length,
                }),
            );

            await this.adapter.deleteDocuments(
                _collection.getId(),
                documents.map((document) => document.getId()),
            );

            return documents;
        });

        return documents;
    }

    /**
     * Cleans the all the collection's documents from the cache
     * And the all related cached documents.
     *
     * @param collectionId string
     *
     * @return boolean
     */
    public async purgeCachedCollection(collectionId: string): Promise<boolean> {
        const { collectionKey } = this.getCacheKeys(collectionId);

        const documentKeys = await this.cache.list(collectionKey);
        for (const documentKey of documentKeys) {
            await this.cache.purge(documentKey);
        }

        await this.cache.purge(collectionKey);

        return true;
    }

    /**
     * Cleans a specific document from cache
     * And related document reference in the collection cache.
     *
     * @param collectionId string
     * @param id string
     *
     * @return boolean
     */
    public async purgeCachedDocument(
        collectionId: string,
        id: string,
    ): Promise<boolean> {
        const { collectionKey, documentKey } = this.getCacheKeys(
            collectionId,
            id,
        );

        await this.cache.purge(collectionKey!, documentKey!);
        await this.cache.purge(documentKey!);

        await this.trigger(
            Database.EVENT_DOCUMENT_PURGE,
            new Document({
                $id: id,
                $collection: collectionId,
            }),
        );

        return true;
    }

    /**
     * Find Documents
     *
     * @return Promise<Document[]>
     * @throws DatabaseException
     * @throws QueryException
     * @throws TimeoutException
     * @throws Exception
     */
    public async find<T extends IRecord>(
        params: FindParams,
    ): Promise<Document<T>[]>;
    public async find<T extends IRecord>(
        collection: string,
        queries?: Query[],
        forPermission?: string,
    ): Promise<Document<T>[]>;
    public async find<T extends IRecord>(
        collectionOrParams: string | FindParams,
        queries: Query[] = [],
        forPermission: string = Database.PERMISSION_READ,
    ): Promise<Document<T>[]> {
        let collection: string;

        if (typeof collectionOrParams === "string") {
            collection = collectionOrParams;
        } else {
            collection = collectionOrParams.collection;
            queries = collectionOrParams.queries ?? [];
            forPermission =
                collectionOrParams.forPermission ?? Database.PERMISSION_READ;
        }

        const _collection = await this.silent(
            async () => await this.getCollection(collection),
        );

        if (!_collection || _collection.isEmpty()) {
            throw new NotFoundException("Collection not found");
        }

        const attributes = _collection.getAttribute("attributes", []);
        const indexes = _collection.getAttribute("indexes", []);

        if (this.validate) {
            const validator = new DocumentsValidator(
                attributes,
                indexes,
                this.maxQueryValues,
                this.adapter.getMinDateTime(),
                this.adapter.getMaxDateTime(),
            );
            if (!validator.isValid(queries)) {
                throw new QueryException(validator.getDescription());
            }
        }

        const authorization = new Authorization(Database.PERMISSION_READ);
        const documentSecurity = _collection.getAttribute(
            "documentSecurity",
            false,
        );
        const skipAuth = authorization.isValid(
            _collection.getPermissionsByType(forPermission),
        );

        if (
            !skipAuth &&
            !documentSecurity &&
            _collection.getId() !== Database.METADATA
        ) {
            throw new AuthorizationException(authorization.getDescription());
        }

        const relationships = attributes.filter(
            (attribute: Document) =>
                attribute.getAttribute("type") === Database.VAR_RELATIONSHIP,
        );

        const grouped = Query.groupByType(queries);
        const filters = grouped["filters"];
        const selects = grouped["selections"];
        let limit = grouped["limit"];
        const offset = grouped["offset"];
        const orderAttributes = grouped["orderAttributes"];
        const orderTypes = grouped["orderTypes"];
        let cursor = grouped["cursor"];
        const cursorDirection = grouped["cursorDirection"];

        if (cursor && cursor.getCollection() !== _collection.getId()) {
            throw new DatabaseException(
                "cursor Document must be from the same Collection.",
            );
        }

        cursor = cursor
            ? (await this.encode(_collection, cursor)).toObject()
            : [];

        queries = [
            ...selects,
            ...Database.convertQueries(_collection, filters),
        ];

        const selections = this.validateSelections(_collection, selects);
        const nestedSelections: Query[] = [];

        for (let index = 0; index < queries.length; index++) {
            const query = queries[index];
            switch (query?.getMethod()) {
                case Query.TYPE_SELECT:
                    const values = query.getValues();
                    for (
                        let valueIndex = 0;
                        valueIndex < values.length;
                        valueIndex++
                    ) {
                        const value = values[valueIndex];
                        if (value.includes(".")) {
                            nestedSelections.push(
                                Query.select([
                                    value.split(".").slice(1).join("."),
                                ]),
                            );

                            const key = value.split(".")[0];

                            for (const relationship of relationships) {
                                if (relationship.getAttribute("key") === key) {
                                    switch (
                                        relationship.getAttribute("options")
                                            .relationType
                                    ) {
                                        case Database.RELATION_MANY_TO_MANY:
                                        case Database.RELATION_ONE_TO_MANY:
                                            values.splice(valueIndex, 1);
                                            break;

                                        case Database.RELATION_MANY_TO_ONE:
                                        case Database.RELATION_ONE_TO_ONE:
                                            values[valueIndex] = key;
                                            break;
                                    }
                                }
                            }
                        }
                    }
                    query.setValues(values);
                    break;
                default:
                    if (query?.getAttribute().includes(".")) {
                        queries.splice(index, 1);
                    }
                    break;
            }
        }

        const getResults = async () =>
            await this.adapter.find(
                _collection.getId(),
                queries,
                limit ?? 25,
                offset ?? 0,
                orderAttributes,
                orderTypes,
                cursor,
                cursorDirection ?? (Database.CURSOR_AFTER as any),
                forPermission,
            );

        const results = skipAuth
            ? await Authorization.skip(getResults)
            : await getResults();

        for (let node of results) {
            if (
                this.resolveRelationships &&
                (!selects.length || nestedSelections.length)
            ) {
                node = await this.silent(
                    async () =>
                        await this.populateDocumentRelationships(
                            _collection,
                            node,
                            nestedSelections,
                        ),
                );
            }
            node = this.casting(_collection, node);
            node = await this.decode(_collection, node, selections);

            if (!node.isEmpty()) {
                node.setAttribute("$collection", _collection.getId());
            }
        }

        for (const query of queries) {
            if (query.getMethod() === Query.TYPE_SELECT) {
                const values = query.getValues();
                for (const result of results) {
                    for (const internalAttribute of this.getInternalAttributes()) {
                        if (!values.includes(internalAttribute["$id"])) {
                            result.removeAttribute(internalAttribute["$id"]);
                        }
                    }
                }
            }
        }

        await this.trigger(Database.EVENT_DOCUMENT_FIND, results);

        return results;
    }

    /**
     * Finds one document in the collection based on the provided queries.
     *
     * @param collection - The name of the collection.
     * @param queries - An array of Query objects.
     * @returns A Document object.
     * @throws DatabaseException
     */
    public async findOne<T extends IRecord>(
        collection: string,
        queries: Query[] = [],
    ): Promise<Document<T>> {
        const results = await this.silent(
            async () =>
                await this.find<T>(collection, [Query.limit(1), ...queries]),
        );

        const found = results[0];

        await this.trigger(Database.EVENT_DOCUMENT_FIND, found);

        if (!found) {
            return new Document();
        }

        return found;
    }

    /**
     * Count Documents
     *
     * Count the number of documents.
     *
     * @param collection - The name of the collection.
     * @param queries - An array of Query objects.
     * @param max - The maximum number of documents to count.
     *
     * @returns The number of documents.
     * @throws DatabaseException
     */
    public async count(
        collection: string,
        queries: Query[] = [],
        max: number | null = null,
    ): Promise<number> {
        const collectionData = await this.silent(
            async () => await this.getCollection(collection),
        );
        const attributes = collectionData.getAttribute("attributes", []);
        const indexes = collectionData.getAttribute("indexes", []);

        if (this.validate) {
            const validator = new DocumentsValidator(
                attributes,
                indexes,
                this.maxQueryValues,
                this.adapter.getMinDateTime(),
                this.adapter.getMaxDateTime(),
            );
            if (!validator.isValid(queries)) {
                throw new QueryException(validator.getDescription());
            }
        }

        const authorization = new Authorization(Database.PERMISSION_READ);
        const skipAuth = authorization.isValid(collectionData.getRead());

        queries = Query.groupByType(queries)["filters"];
        queries = Database.convertQueries(collectionData, queries);

        const getCount = async () =>
            await this.adapter.count(collectionData.getId(), queries, max);
        const count = skipAuth
            ? await Authorization.skip(getCount)
            : await getCount();

        await this.trigger(Database.EVENT_DOCUMENT_COUNT, count);

        return count;
    }

    /**
     * Sum an attribute
     *
     * Sum an attribute for all the documents. Pass max=0 for unlimited count
     *
     * @param collection - The name of the collection.
     * @param attribute - The attribute to sum.
     * @param queries - An array of Query objects.
     * @param max - The maximum number of documents to sum.
     *
     * @returns The sum of the attribute.
     * @throws DatabaseException
     */
    public async sum(
        collection: string,
        attribute: string,
        queries: Query[] = [],
        max: number | null = null,
    ): Promise<number> {
        const collectionData = await this.silent(
            async () => await this.getCollection(collection),
        );
        const attributes = collectionData.getAttribute("attributes", []);
        const indexes = collectionData.getAttribute("indexes", []);

        if (this.validate) {
            const validator = new DocumentsValidator(
                attributes,
                indexes,
                this.maxQueryValues,
                this.adapter.getMinDateTime(),
                this.adapter.getMaxDateTime(),
            );
            if (!validator.isValid(queries)) {
                throw new QueryException(validator.getDescription());
            }
        }

        queries = Database.convertQueries(collectionData, queries);

        const sum = await this.adapter.sum(
            collectionData.getId(),
            attribute,
            queries,
            max,
        );

        await this.trigger(Database.EVENT_DOCUMENT_SUM, sum);

        return sum;
    }

    /**
     * Add Attribute Filter
     *
     * @param name - The name of the filter.
     * @param encode - The encode function.
     * @param decode - The decode function.
     *
     * @returns void
     */
    public static addFilter(name: string, filter: Filter): void {
        this.filters[name] = filter;
    }

    /**
     * Encode Document
     *
     * @param collection - The collection document.
     * @param document - The document to encode.
     *
     * @returns The encoded document.
     * @throws DatabaseException
     */
    public async encode(
        collection: Document,
        document: Document,
    ): Promise<Document> {
        const attributes = collection.getAttribute("attributes", []);

        const internalAttributes = Database.INTERNAL_ATTRIBUTES.filter(
            (attribute) => {
                // We don't want to encode permissions into a JSON string
                return attribute["$id"] !== "$permissions";
            },
        );

        const allAttributes = [
            ...attributes,
            ...internalAttributes.map((v) => new Document(v as any)),
        ];

        for (const attribute of allAttributes) {
            const key = attribute.getAttribute("$id");
            const array = attribute.getAttribute("array", false);
            const defaultValue = attribute.getAttribute("default", null);
            const filters = attribute.getAttribute("filters", []);
            let value = document.getAttribute(key);

            // Continue on optional param with no default
            if (value === null && defaultValue === null) {
                continue;
            }

            // Assign default only if no value provided
            if (value === null && defaultValue !== null) {
                value = array ? defaultValue : [defaultValue];
            } else {
                value = array ? value : [value];
            }

            value = await Promise.all(
                value.map(async (node: any) => {
                    if (node !== null) {
                        for (const filter of filters) {
                            node = await this.encodeAttribute(
                                filter,
                                node,
                                document,
                            );
                        }
                    }
                    return node;
                }),
            );

            if (!array) {
                value = value[0];
            }

            document.setAttribute(key, value);
        }

        return document;
    }

    /**
     * Decode Document
     *
     * @param collection - The collection document.
     * @param document - The document to decode.
     * @param selections - An array of selected attributes.
     * @returns The decoded document.
     * @throws DatabaseException
     */
    public async decode(
        collection: Document,
        document: Document,
        selections: string[] = [],
    ): Promise<Document> {
        const attributes = collection
            .getAttribute("attributes", [])
            .filter(
                (attribute: any) =>
                    attribute.getAttribute("type") !==
                    Database.VAR_RELATIONSHIP,
            );

        const relationships = collection
            .getAttribute("attributes", [])
            .filter(
                (attribute: any) =>
                    attribute.getAttribute("type") ===
                    Database.VAR_RELATIONSHIP,
            );

        for (const relationship of relationships) {
            const key = relationship.getAttribute("$id");

            if (
                document.hasOwnProperty(key) ||
                document.hasOwnProperty(this.adapter.filter(key))
            ) {
                let value = document.getAttribute(key);
                value =
                    value ?? document.getAttribute(this.adapter.filter(key));
                document.removeAttribute(this.adapter.filter(key));
                document.setAttribute(key, value);
            }
        }

        const allAttributes = [
            ...attributes,
            ...this.getInternalAttributes().map((v) => new Document(v)),
        ];
        for (const attribute of allAttributes) {
            const key = attribute.getAttribute(
                "key",
                attribute.getAttribute("$id"),
            );
            const array = attribute.getAttribute("array", false);
            const filters = attribute.getAttribute("filters", []);
            let value = document.getAttribute(key);

            if (value === null) {
                value = document.getAttribute(this.adapter.filter(key));

                if (value !== null) {
                    document.removeAttribute(this.adapter.filter(key));
                }
            }

            if (array) {
                if (typeof value === "string") {
                    try {
                        value = JSON.parse(value);
                    } catch {
                        value = [value];
                    }
                } else {
                    value = value;
                }
            } else {
                value = [value];
            }

            value = value === null || value === undefined ? [] : value;

            value = await Promise.all(
                value.map(async (val: any) => {
                    for (const filter of filters.reverse()) {
                        val = await this.decodeAttribute(filter, val, document);
                    }
                    return val;
                }),
            );

            if (
                selections.length === 0 ||
                selections.includes(key) ||
                selections.includes("*")
            ) {
                if (
                    selections.length === 0 ||
                    selections.includes(key) ||
                    selections.includes("*") ||
                    ["$createdAt", "$updatedAt"].includes(key)
                ) {
                    if (
                        ["$createdAt", "$updatedAt"].includes(key) &&
                        value[0] === null
                    ) {
                        continue;
                    } else {
                        document.setAttribute(key, array ? value : value[0]);
                    }
                }
            }
        }

        return document;
    }

    /**
     * Casting
     *
     * @param collection Document
     * @param document Document
     *
     * @return Document
     */
    public casting(collection: Document, document: Document): Document {
        if (this.adapter.getSupportForCasting()) {
            return document;
        }

        const attributes = collection.getAttribute("attributes", []);

        for (const attribute of attributes) {
            const key = attribute.getId();
            const type = attribute.getAttribute("type");
            const array = attribute.getAttribute("array", false);
            let value = document.getAttribute(key, null);
            if (value === null && type !== Database.VAR_BOOLEAN) {
                continue;
            }

            if (array) {
                value = typeof value !== "string" ? value : JSON.parse(value);
            } else {
                value = [value];
            }

            for (let i = 0; i < value.length; i++) {
                switch (type) {
                    case Database.VAR_BOOLEAN:
                        value[i] = Boolean(value[i]);
                        break;
                    case Database.VAR_INTEGER:
                        value[i] = Number(value[i]);
                        break;
                    case Database.VAR_FLOAT:
                        value[i] = parseFloat(value[i]);
                        break;
                    default:
                        break;
                }
            }

            document.setAttribute(key, array ? value : value[0]);
        }

        return document;
    }

    /**
     * Encode Attribute
     *
     * Passes the attribute value, and document context to a predefined filter
     * that allows you to manipulate the input format of the given attribute.
     *
     * @param name string
     * @param value any
     * @param document Document
     *
     * @return any
     * @throws DatabaseException
     */
    protected async encodeAttribute(
        name: string,
        value: any,
        document: Document,
    ): Promise<any> {
        if (!(name in this.instanceFilters) && !(name in Database.filters)) {
            throw new NotFoundException(`Filter: ${name} not found`);
        }

        try {
            if (name in this.instanceFilters) {
                value = await this.instanceFilters[name]!.encode(
                    value,
                    document,
                    this,
                );
            } else {
                value = await Database.filters[name]!.encode(
                    value,
                    document,
                    this,
                );
            }
        } catch (error: any) {
            throw new DatabaseException(error.message, error.code, error);
        }

        return value;
    }

    /**
     * Decode Attribute
     *
     * Passes the attribute value, and document context to a predefined filter
     * that allows you to manipulate the output format of the given attribute.
     *
     * @param name string
     * @param value any
     * @param document Document
     *
     * @return any
     * @throws DatabaseException
     */
    protected async decodeAttribute(
        name: string,
        value: any,
        document: Document,
    ): Promise<any> {
        if (!this.filter) {
            return value;
        }

        if (!(name in this.instanceFilters) && !(name in Database.filters)) {
            throw new NotFoundException("Filter not found");
        }

        if (name in this.instanceFilters) {
            value = await this.instanceFilters[name]!.decode(
                value,
                document,
                this,
            );
        } else {
            value = await Database.filters[name]!.decode(value, document, this);
        }

        return value;
    }

    /**
     * Validate if a set of attributes can be selected from the collection
     *
     * @param collection Document
     * @param queries Array<Query>
     * @return Array<string>
     * @throws QueryException
     */
    private validateSelections(
        collection: Document,
        queries: Array<Query>,
    ): Array<string> {
        if (queries.length === 0) {
            return [];
        }

        let selections: Array<string> = [];
        let relationshipSelections: Array<string> = [];

        for (const query of queries) {
            if (query.getMethod() === Query.TYPE_SELECT) {
                for (const value of query.getValues()) {
                    if (value.includes(".")) {
                        relationshipSelections.push(value);
                        continue;
                    }
                    selections.push(value);
                }
            }
        }

        // Allow querying internal attributes
        const keys = this.getInternalAttributes().map(
            (attribute) => attribute["$id"],
        );

        for (const attribute of collection.getAttribute("attributes", [])) {
            if (attribute.getAttribute("type") !== Database.VAR_RELATIONSHIP) {
                // Fallback to $id when key property is not present in metadata table for some tables such as Indexes or Attributes
                keys.push(attribute.getAttribute("key") ?? attribute.getId());
            }
        }

        const invalid = selections.filter(
            (selection) => !keys.includes(selection),
        );
        if (invalid.length > 0 && !invalid.includes("*")) {
            throw new QueryException(
                "Cannot select attributes: " + invalid.join(", "),
            );
        }

        selections = selections.concat(relationshipSelections);

        selections.push("$id");
        selections.push("$internalId");
        selections.push("$collection");
        selections.push("$createdAt");
        selections.push("$updatedAt");
        selections.push("$permissions");

        return selections;
    }

    /**
     * Get adapter attribute limit, accounting for internal metadata
     * Returns 0 to indicate no limit
     *
     * @return number
     */
    public getLimitForAttributes(): number {
        // If negative, return 0
        // -1 ==> virtual columns count as total, so treat as buffer
        return Math.max(
            this.adapter.getLimitForAttributes() -
                this.adapter.getCountOfDefaultAttributes() -
                1,
            0,
        );
    }

    /**
     * Get adapter index limit
     *
     * @return number
     */
    public getLimitForIndexes(): number {
        return (
            this.adapter.getLimitForIndexes() -
            this.adapter.getCountOfDefaultIndexes()
        );
    }

    /**
     * @param collection Document
     * @param queries Array<Query>
     * @return Array<Query>
     * @throws QueryException
     * @throws Exception
     */
    public static convertQueries(
        collection: Document,
        queries: Array<Query>,
    ): Array<Query> {
        let attributes = collection.getAttribute("attributes", []);

        for (const attribute of Database.INTERNAL_ATTRIBUTES) {
            attributes.push(new Document(attribute as any));
        }

        for (const attribute of attributes) {
            for (const query of queries) {
                if (query.getAttribute() === attribute.getId()) {
                    query.setOnArray(attribute.getAttribute("array", false));
                }
            }

            if (attribute.getAttribute("type") === Database.VAR_DATETIME) {
                for (let index = 0; index < queries.length; index++) {
                    const query = queries[index]!;
                    if (query.getAttribute() === attribute.getId()) {
                        let values = query.getValues();
                        for (
                            let valueIndex = 0;
                            valueIndex < values.length;
                            valueIndex++
                        ) {
                            try {
                                values[valueIndex] = DateTime.setTimezone(
                                    values[valueIndex],
                                );
                            } catch (e: any) {
                                throw new QueryException(e.message, e?.code, e);
                            }
                        }
                        query.setValues(values);
                        queries[index] = query;
                    }
                }
            }
        }

        return queries;
    }

    /**
     * @param collection Document
     * @param id string
     * @return void
     * @throws DatabaseException
     */
    private async purgeRelatedDocuments(
        collection: Document,
        id: string,
    ): Promise<void> {
        if (collection.getId() === Database.METADATA) {
            return;
        }

        const relationships = collection
            .getAttribute("attributes", [])
            .filter(
                (attribute: any) =>
                    attribute["type"] === Database.VAR_RELATIONSHIP,
            );

        if (relationships.length === 0) {
            return;
        }

        const key = `map:${collection.getId()}:${id}`;
        const cache = await this.getCacheValue(key);
        if ((cache?.length ?? 0) > 0) {
            for (const v of cache!) {
                const [collectionId, documentId] = v.split(":");
                await this.purgeCachedDocument(collectionId, documentId);
            }
            await this.setCacheValue(key, []);
        }
        return;
    }

    /**
     * @return Array<Array<string, any>>
     */
    public getInternalAttributes(): Array<{ [key: string]: any }> {
        let attributes = Database.INTERNAL_ATTRIBUTES;

        if (!this.adapter.getSharedTables()) {
            attributes = Database.INTERNAL_ATTRIBUTES.filter(
                (attribute: any) => attribute["$id"] !== "$tenant",
            );
        }

        return attributes;
    }

    public getRepository<Entity extends { [key: string]: any }>(
        target: any,
    ): Repository<Entity> {
        const repoMap = this.repositories.get(target);

        if (repoMap) {
            return repoMap as Repository<any>;
        }

        const repo = new Repository<any>(target, this);
        this.repositories.set(target, repo);

        return repo;
    }

    // public async genrateMigration(): Promise<void> {

    //   if (!this.entities?.length) {
    //     this.logger.error('No entities found');
    //     return;
    //   }

    //   this.logger.info('Generating migration...');

    //   const entities = this.entities.map(entity =>
    //     this.getRepository(entity).getMeta()
    //   );

    //   this.logger.debug(entities)

    //   MigrationGenerator.generateMigrationFile(entities, `Migration_${new Date().getTime()}`);
    // }

    public async applyMigration(stopOnErrors: boolean = false): Promise<void> {
        if (!this.entities.length) {
            this.logger.error("No entities found");
            return;
        }

        this.logger.info("Applying migration...");

        const entities = this.entities.map((entity) =>
            this.getRepository(entity).getMeta(),
        );

        for (const entity of entities) {
            this.logger.info("SELECTED: ", entity?.getId());

            try {
                await this.createCollection(
                    entity.getId(),
                    entity.getAttribute("attributes", []),
                    entity.getAttribute("indexes", []),
                    entity.getAttribute("$permissions", undefined),
                    entity.getAttribute("documentSecurity", false),
                );

                this.logger.info("Collection Created: ", entity.getId());
            } catch (e) {
                if (e instanceof DuplicateException) {
                    this.logger.warn("Collection Already Exists?");
                } else this.logger.error(e);

                if (stopOnErrors) {
                    throw e;
                }
            }
        }

        this.logger.info("Migration applied successfully");
    }

    /**
     * Set the cache TTL (Time To Live) in seconds
     *
     * @param ttl
     * @return this
     */
    public setCacheTTL(ttl: number): this {
        this.cacheTTL = ttl;
        return this;
    }

    /**
     * Get the cache TTL (Time To Live) in seconds
     *
     * @return number
     */
    public getCacheTTL(): number {
        return this.cacheTTL;
    }

    /**
     * Enable cache
     *
     * @return this
     */
    public enableCache(): this {
        this.cacheEnabled = true;
        return this;
    }

    /**
     * Disable cache
     *
     * @return this
     */
    public disableCache(): this {
        this.cacheEnabled = false;
        return this;
    }

    /**
     * Check if cache is enabled
     *
     * @return boolean
     */
    public isCacheEnabled(): boolean {
        return this.cacheEnabled;
    }

    /**
     * Set whether caching is enabled
     *
     * @param enabled
     * @return this
     */
    public setCacheEnabled(enabled: boolean): this {
        this.cacheEnabled = enabled;
        return this;
    }

    /**
     * Skip cache usage
     *
     * Execute a callback without using cache
     *
     * @template T
     * @param callback
     * @return T
     */
    public async skipCache<T>(callback: () => Promise<T>): Promise<T> {
        const initial = this.cacheEnabled;
        this.disableCache();

        try {
            return await callback();
        } finally {
            this.cacheEnabled = initial;
        }
    }

    /**
     * Clear entire cache for this database
     *
     * @param pattern - Optional pattern to selectively clear cache (defaults to all)
     * @return Promise<boolean>
     */
    // public async clearCache(pattern?: string): Promise<boolean> {
    //     const tenantId = this.adapter.getTenantId() ?? "default";
    //     const cacheKey = pattern
    //         ? `${this.cacheName}-cache-${this.getPrefix()}:${tenantId}:${pattern}`
    //         : `${this.cacheName}-cache-${this.getPrefix()}:${tenantId}:*`;

    //     const keys = await this.cache.list(cacheKey);
    //     console.log(
    //         "++++++++++++++++++++",
    //         await (this.cache as any).list(),
    //         "_________+++++___",
    //     );
    //     console.log(keys, "creaeCache"); //#debug
    //     for (const key of keys) {
    //         await this.cache.purge(key);
    //     }

    //     return true;
    // }

    /**
     * Generate standard cache key format
     *
     * @param key - The unique identifier part of the cache key
     * @return string - Formatted cache key
     */
    protected generateCacheKey(key: string): string {
        return `${this.cacheName}-cache-${this.getPrefix()}:${this.adapter.getTenantId() ?? "default"}:${key}`;
    }

    /**
     * Set value in cache with TTL
     *
     * @param key - Key identifier
     * @param value - Value to store
     * @param ttl - Optional time-to-live in seconds (defaults to configured cacheTTL)
     * @return Promise<boolean>
     */
    protected async setCacheValue(key: string, value: any): Promise<boolean> {
        if (!this.cacheEnabled) {
            return false;
        }

        try {
            const cacheKey = key.includes(this.cacheName)
                ? key
                : this.generateCacheKey(key);
            return (await this.cache.save(cacheKey, value)) as boolean;
        } catch (error) {
            this.logger.error("Cache set error:", error);
            return false;
        }
    }

    /**
     * Get value from cache
     *
     * @param key - Key identifier
     * @return Promise<any>
     */
    protected async getCacheValue(key: string, ttl?: number): Promise<any> {
        if (!this.cacheEnabled) {
            return null;
        }

        try {
            const cacheKey = key.includes(this.cacheName)
                ? key
                : this.generateCacheKey(key);
            return await this.cache.load(cacheKey, ttl ?? this.cacheTTL);
        } catch (error) {
            this.logger.error("Cache get error:", error);
            return null;
        }
    }

    /**
     * Generate cache keys for collection and document caching
     *
     * @param collectionId - The collection ID
     * @param documentId - Optional document ID
     * @param selects - Array of selected attributes
     */
    public getCacheKeys(
        collectionId: string,
        documentId?: string | null,
        selects: string[] = [],
    ): {
        collectionKey: string;
        documentKey: string | null;
        hashKey: string | null;
    } {
        let hostname: string | undefined;

        // if (this.adapter.getSupportForHostname()) {
        //     hostname = this.adapter.getHostname();
        // }

        let tenantSegment: string | number | null = this.adapter.getTenantId();

        // Check if this is a global collection that doesn't use tenant segmentation
        if (
            collectionId === Database.METADATA &&
            this.globalCollections?.[documentId as string]
        ) {
            tenantSegment = null;
        }

        const collectionKey = `${this.cacheName}-cache-${hostname ?? ""}:${this.getPrefix()}:${tenantSegment}:collection:${collectionId}`;

        let documentKey: string | null = null;
        let documentHashKey: string | null = null;

        if (documentId) {
            documentKey = documentHashKey = `${collectionKey}:${documentId}`;

            if (selects.length > 0) {
                const selectsHash = crypto
                    .createHash("md5")
                    .update(selects.join(""))
                    .digest("hex");
                documentHashKey = `${documentKey}:${selectsHash}`;
            }
        }

        return { collectionKey, documentKey, hashKey: documentHashKey };
    }
}
