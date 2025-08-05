import { AttributeEnum, EventsEnum, PermissionEnum, RelationEnum, RelationSideEnum } from "./enums.js";
import { Attribute, Collection } from "@validators/schema.js";
import { Adapter } from "@adapters/base.js";
import { CreateCollection, Filters, UpdateCollection } from "./types.js";
import { Cache } from "./cache.js";
import { Cache as NuvixCache } from '@nuvix/cache';
import { Entities, IEntity } from "types.js";
import { QueryBuilder } from "@utils/query-builder.js";
import { Query } from "./query.js";
import { Doc } from "./doc.js";
import { AuthorizationException, DatabaseException, DuplicateException, IndexException, LimitException, NotFoundException, QueryException, StructureException } from "@errors/index.js";
import { Permission } from "@utils/permission.js";
import { Role } from "@utils/role.js";
import { Permissions } from "@validators/permissions.js";
import { Index } from "@validators/index-validator.js";
import { Documents } from "@validators/queries/documents.js";
import { Authorization } from "@utils/authorization.js";
import { ID } from "@utils/id.js";
import { Structure } from "@validators/structure.js";

export class Database extends Cache {
    constructor(adapter: Adapter, cache: NuvixCache, options: DatabaseOptions = {}) {
        super(adapter, cache, options);
    }

    /**
     * Creates a new database.
     */
    public async create(database?: string): Promise<void> {
        database = database ?? this.adapter.$database;
        await this.adapter.create(database);

        const attributes = [...Database.COLLECTION.attributes]
            .map((attr) => new Doc(attr));
        await this.silent(() => this.createCollection({ id: Database.METADATA, attributes }));

        this.trigger(EventsEnum.DatabaseCreate, database);
    }

    public async exists(database?: string, collection?: string): Promise<boolean> {
        database ??= this.adapter.$database;
        return this.adapter.exists(database, collection);
    }

    public async list(): Promise<string[]> {
        this.trigger(EventsEnum.DatabaseList, []);
        return [];
    }

    public async delete(database?: string): Promise<void> {
        database ??= this.adapter.$database;
        await this.adapter.delete(database);

        this.trigger(EventsEnum.DatabaseDelete, database);
        await this.cache.flush();
    }

    /**
     * Creates a new collection in the database.
     */
    public async createCollection(
        { id, attributes = [], indexes = [], permissions, documentSecurity }:
            CreateCollection
    ): Promise<Doc<Collection>> {
        permissions ??= [
            Permission.create(Role.any()),
        ];

        if (this.validate) {
            const perms = new Permissions();
            if (!perms.$valid(permissions)) {
                throw new DatabaseException(perms.$description);
            }
        }

        let collection = await this.silent(() => this.getCollection(id));
        if (!collection.empty() && id !== Database.METADATA) {
            throw new DuplicateException(`Collection '${id}' already exists.`);
        }

        // Fix metadata index length & orders
        for (let i = 0; i < indexes.length; i++) {
            const index = indexes[i]!;
            const orders: (string | null)[] = index.get('orders', []);

            const indexAttributes = index.get('attributes', []);
            for (let j = 0; j < indexAttributes.length; j++) {
                const attr = indexAttributes[j];
                for (const collectionAttribute of attributes) {
                    if (collectionAttribute.get('$id') === attr) {
                        const isArray = collectionAttribute.get('array', false);
                        if (isArray) {
                            orders[j] = null;
                        }
                        break;
                    }
                }
            }

            index.set('orders', orders);
            indexes[i] = index;
        }

        collection = new Doc<Collection>({
            '$id': id,
            '$permissions': permissions,
            'name': id,
            'attributes': attributes,
            'indexes': indexes,
            'documentSecurity': documentSecurity
        });

        if (this.validate) {
            const validator = new Index(
                attributes,
                this.adapter.$maxIndexLength,
                this.adapter.$internalIndexesKeys,
                this.adapter.$supportForIndexArray,
            );
            indexes.forEach((index) => {
                if (!validator.$valid(index)) {
                    throw new IndexException(validator.$description);
                }
            });
        }

        if (indexes.length && this.adapter.getCountOfIndexes(collection) > this.adapter.$limitForIndexes) {
            throw new LimitException(`Index limit of ${this.adapter.$limitForIndexes} exceeded. Cannot create collection.`);
        }

        if (attributes.length) {
            if (this.adapter.$limitForAttributes && attributes.length > this.adapter.$limitForAttributes) {
                throw new LimitException(`Attribute limit of ${this.adapter.$limitForAttributes} exceeded. Cannot create collection.`);
            }
            if (this.adapter.$documentSizeLimit && this.adapter.getAttributeWidth(collection) > this.adapter.$documentSizeLimit) {
                throw new LimitException(`Document size limit of ${this.adapter.$documentSizeLimit} exceeded. Cannot create collection.`);
            }
        }

        try {
            await this.adapter.createCollection({ name: id, attributes, indexes });
        } catch (error) {
            if (error instanceof DuplicateException) {
                // $HACK: Metadata should still be updated, can be removed when null tenant collections are supported.
                if (!this.sharedTables || !this.migrating) {
                    throw error;
                }
            } else {
                throw error;
            }
        }

        if (id === Database.METADATA) return new Doc(Database.COLLECTION);

        const createdCollection = await this.silent(() => this.createDocument(Database.METADATA, collection));
        this.trigger(EventsEnum.CollectionCreate, createdCollection);

        return createdCollection;
    }

    public async updateCollection(
        { id, documentSecurity, permissions }: UpdateCollection
    ): Promise<Doc<Collection>> {
        if (permissions !== undefined) {
            if (this.validate) {
                const perms = new Permissions();
                if (!perms.$valid(permissions)) {
                    throw new DatabaseException(perms.$description);
                }
            }
        }

        let collection = await this.silent(() => this.getCollection(id));

        if (collection.empty()) {
            throw new NotFoundException(`Collection "${id}" not found`);
        }

        if (
            this.adapter.$sharedTables
            && collection.getTenant() !== this.adapter.$tenantId
        ) {
            throw new NotFoundException(`Collection "${id}" not found`);
        }

        if (permissions !== undefined) collection.set('$permissions', permissions);
        if (documentSecurity !== undefined) collection.set('documentSecurity', documentSecurity);

        // collection = await this.silent(() => this.updateDocument(Database.METADATA, collection.getId(), collection));
        this.trigger(EventsEnum.CollectionUpdate, collection);

        return collection;
    }

    /**
     * Retrieves a collection by its ID.
     * If the collection is not found or does not match the tenant ID, an empty Doc
     */
    public async getCollection(id: string, throwOnNotFound?: boolean): Promise<Doc<Collection>> {
        const collection = await this.silent(() => this.getDocument<Collection>(Database.METADATA, id));

        if (id !== Database.METADATA
            && this.adapter.$sharedTables
            && collection.getTenant() !== null
            && collection.getTenant() !== this.adapter.$tenantId
        ) {
            if (throwOnNotFound) {
                throw new NotFoundException(`Collection '${id}' not found`);
            }
            return new Doc<Collection>();
        }

        this.trigger(EventsEnum.CollectionRead, collection);
        if (collection.empty() && throwOnNotFound) {
            throw new NotFoundException(`Collection '${id}' not found`);
        }

        return collection;
    }

    /**
     * Lists all collections in the database.
     */
    public async listCollections(
        limit: number = 25,
        offset: number = 0,
    ): Promise<Doc<Collection>[]> {
        const query = [
            Query.limit(limit),
            Query.offset(offset)
        ];

        // return this.find<Collection>(Database.METADATA, query);
        return []
    }

    /**
     * Gets the size of a collection.
     */
    public async getSizeOfCollection(collectionId: string): Promise<number> {
        const collection = await this.silent(() => this.getCollection(collectionId, true));

        if (this.adapter.$sharedTables && collection.getTenant() !== this.adapter.$tenantId) {
            throw new NotFoundException(`Collection '${collectionId}' not found`);
        }

        return this.adapter.getSizeOfCollection(collection.getId());
    }

    public async getSizeOfCollectionOnDisk(collectionId: string): Promise<number> {
        if (this.adapter.$sharedTables && !this.adapter.$tenantId) {
            throw new DatabaseException('Missing tenant. Tenant must be set when table sharing is enabled.');
        }

        const collection = await this.silent(() => this.getCollection(collectionId, true));

        if (this.adapter.$sharedTables && collection.getTenant() !== this.adapter.$tenantId) {
            throw new NotFoundException(`Collection '${collectionId}' not found`);
        }

        return this.adapter.getSizeOfCollectionOnDisk(collection.getId());
    }

    public async analyzeCollection(collection: string): Promise<boolean> {
        return this.adapter.analyzeCollection(collection);
    }

    public async deleteCollection(id: string): Promise<boolean> {
        const collection = await this.silent(() => this.getDocument(Database.METADATA, id));

        if (collection.empty()) {
            throw new NotFoundException(`Collection '${id}' not found`);
        }

        if (this.adapter.$sharedTables && collection.getTenant() !== this.adapter.$tenantId) {
            throw new NotFoundException(`Collection '${id}' not found`);
        }

        const relationships = (collection.get('attributes') ?? []).filter(
            (attribute) => attribute.get('type') === AttributeEnum.Relationship
        );

        // for (const relationship of relationships) {
        //     await this.deleteRelationship(collection.getId(), relationship.get('$id'));
        // }

        try {
            await this.adapter.deleteCollection(id);
        } catch (error) {
            if (error instanceof NotFoundException) {
                // HACK: Metadata should still be updated, can be removed when null tenant collections are supported.
                if (!this.adapter.$sharedTables || !this.migrating) {
                    throw error;
                }
            } else {
                throw error;
            }
        }

        let deleted: boolean;
        if (id === Database.METADATA) {
            deleted = true;
        } else {
            deleted = false
            // deleted = await this.silent(() => this.deleteDocument(Database.METADATA, id));
        }

        if (deleted) {
            this.trigger(EventsEnum.CollectionDelete, collection);
        }

        // await this.purgeCachedCollection(id);

        return deleted;
    }

    public async getDocument<C extends (string & keyof Entities) | Partial<IEntity> & Record<string, any>>(
        collectionId: C extends string ? C : string,
        id: string,
        query: ((builder: QueryBuilder) => QueryBuilder) | Query[] = [],
        forUpdate: boolean = false,
    ): Promise<C extends string ? Doc<Entities[C]> : C extends Record<string, any> ? Doc<C> : Doc<IEntity>> {
        if (collectionId === Database.METADATA && id === Database.METADATA) {
            return new Doc(Database.COLLECTION) as any;
        }

        if (!collectionId) {
            throw new NotFoundException(`Collection '${collectionId}' not found.`);
        }
        if (!id) return new Doc() as any;

        const collection = await this.silent(() => this.getCollection(collectionId, true));
        const attributes = collection.get('attributes', []);

        const processedQuery = await this.processQueries(query, collection);

        const results = await this.adapter.getDocument(collection.getId(), id, processedQuery);

        return {} as any;
    }

    public async createDocument<C extends (string & keyof Entities) | Partial<IEntity> & Record<string, any>>(
        collectionId: C extends string ? C : string,
        document: C extends string ? Partial<Entities[C]> | Doc<Partial<Entities[C]>> : C extends Record<string, any> ? Partial<C> | Doc<Partial<C>> : Partial<IEntity>
    ): Promise<C extends string ? Doc<Entities[C]> : C extends Record<string, any> ? Doc<C> : Doc<IEntity>> {
        if (
            collectionId !== Database.METADATA
            && this.adapter.$sharedTables
            && !this.adapter.$tenantPerDocument
            && !this.adapter.$tenantId
        ) {
            throw new DatabaseException('Missing tenant. Tenant must be set when table sharing is enabled.');
        }

        if (
            !this.adapter.$sharedTables
            && this.adapter.$tenantPerDocument
        ) {
            throw new DatabaseException('Shared tables must be enabled if tenant per document is enabled.');
        }

        const collection = await this.silent(() => this.getCollection(collectionId));

        if (collection.getId() !== Database.METADATA) {
            const authorization = new Authorization(PermissionEnum.Create);
            if (!authorization.$valid(collection.getCreate())) {
                throw new AuthorizationException(authorization.$description);
            }
        }

        const time = new Date().toISOString();
        let doc = new Doc(document);

        const createdAt = doc.get('$createdAt');
        const updatedAt = doc.get('$updatedAt');

        doc
            .set('$id', doc.getId() || ID.unique())
            .set('$collection', collection.getId())
            .set('$createdAt', (createdAt === null || createdAt === undefined || !this.preserveDates) ? time : createdAt)
            .set('$updatedAt', (updatedAt === null || updatedAt === undefined || !this.preserveDates) ? time : updatedAt);

        if (this.adapter.$sharedTables) {
            if (this.adapter.$tenantPerDocument) {
                if (
                    collection.getId() !== Database.METADATA
                    && doc.getTenant() === null
                ) {
                    throw new DatabaseException('Missing tenant. Tenant must be set when tenant per document is enabled.');
                }
            } else {
                doc.set('$tenant', this.adapter.$tenantId);
            }
        }

        doc = await this.encode(collection, doc);

        if (this.validate) {
            const validator = new Permissions();
            if (!validator.$valid(doc.get('$permissions', []))) {
                throw new DatabaseException(validator.$description);
            }
        }

        const structure = new Structure(
            collection,
            // this.adapter.getIdAttributeType(),
            // this.adapter.getMinDateTime(),
            // this.adapter.getMaxDateTime(),
        );
        if (!await structure.$valid(doc)) {
            throw new StructureException(structure.$description);
        }

        const result =
            // if (this.resolveRelationships) {
            //     doc = await this.silent(() => this.createDocumentRelationships(collection, doc));
            // }
            await this.adapter.createDocument(collection.getId(), doc as any);

        // if (this.resolveRelationships) {
        //     result = await this.silent(() => this.populateDocumentRelationships(collection, result));
        // }

        // const decodedResult = this.decode(collection, castedResult);

        this.trigger(EventsEnum.DocumentCreate, result);

        return result;
    }

    async processQueries(
        queries: ((builder: QueryBuilder) => QueryBuilder) | Query[],
        collection: Doc<Collection>,
        metadata: Attribute['options'] & {
            populated?: boolean;
        } = {},
    ): Promise<ProcessQuery> {
        if (!Array.isArray(queries)) {
            queries = queries(new QueryBuilder()).build();
        }

        if (this.validate && queries.length) {
            const validator = new Documents(
                collection.get('attributes', []),
                collection.get('indexes', []),
                this.maxQueryValues,
            );
            if (!validator.$valid(queries)) {
                throw new QueryException(validator.$description);
            }
        }

        const { populate: _populate, selections } = Query.groupByType(queries);
        const attributes = collection.get('attributes', []);

        if (selections.length) {
            const attributeMap = new Map(attributes.map(attr => [attr.get('$id'), attr]));

            for (const query of selections) {
                const attributeId = query.getAttribute();
                const attribute = attributeMap.get(attributeId);

                if (!attribute) {
                    throw new QueryException(`Attribute '${attributeId}' not found in collection '${collection.getId()}'.`);
                }

                const attributeType = attribute.get('type');
                if (attributeType === AttributeEnum.Relationship || attributeType === AttributeEnum.Virtual) {
                    throw new QueryException(`Attribute '${attributeId}' of type '${attributeType}' cannot be selected directly. Use populate instead.`);
                }
            }
        }

        if (!_populate.size) {
            return {
                queries,
                collection,
                selections: selections.map(q => q.getAttribute())
            };
        }

        const populate: ProcessQuery[] = [];

        for (const [attribute, values] of _populate.entries()) {
            const attributeDoc = attributes.find(attr => attr.get('$id') === attribute);
            if (!attributeDoc) {
                throw new QueryException(`Attribute '${attribute}' not found in collection '${collection.getId()}'.`);
            }

            if (attributeDoc.get('type') !== AttributeEnum.Relationship) {
                throw new QueryException(`Attribute '${attribute}' is not a relationship and cannot be populated.`);
            }

            if (!Array.isArray(values)) {
                throw new QueryException(`Populate query for attribute '${attribute}' must be an array of queries.`);
            }

            const relatedCollectionId = attributeDoc.get('options', {})['relatedCollection'];
            const relatedCollection = await this.silent(() => this.getCollection(relatedCollectionId));
            if (relatedCollection.empty()) {
                throw new NotFoundException(`Collection '${relatedCollectionId}' not found for attribute '${attribute}'.`);
            }

            const processedQueries = await this.processQueries(values, relatedCollection, {
                populated: true,
                ...attributeDoc.get('options', {})
            });
            populate.push(processedQueries);
        }

        return {
            queries,
            collection,
            selections: selections.map(q => q.getAttribute()),
            populate
        };
    }
}

export interface ProcessQuery {
    queries: Query[];
    collection: Doc<Collection>;
    selections?: string[];
    populate?: ProcessQuery[]
}

export type DatabaseOptions = {
    tenant?: number;
    filters?: Filters;
};
