import { AttributeEnum, EventsEnum, PermissionEnum, RelationEnum, RelationSideEnum } from "./enums.js";
import { Attribute, Collection, Index } from "@validators/schema.js";
import { CreateCollection, Filters, UpdateCollection } from "./types.js";
import { Cache } from "./cache.js";
import { Cache as NuvixCache } from '@nuvix/cache';
import { Entities, IEntity } from "types.js";
import { QueryBuilder } from "@utils/query-builder.js";
import { Query } from "./query.js";
import { Doc } from "./doc.js";
import { AuthorizationException, DatabaseException, DependencyException, DuplicateException, IndexException, LimitException, NotFoundException, QueryException, StructureException } from "@errors/index.js";
import { Permission } from "@utils/permission.js";
import { Role } from "@utils/role.js";
import { Permissions } from "@validators/permissions.js";
import { Index as IndexValidator } from "@validators/index-validator.js";
import { Documents } from "@validators/queries/documents.js";
import { Authorization } from "@utils/authorization.js";
import { ID } from "@utils/id.js";
import { Structure } from "@validators/structure.js";
import { Adapter } from "@adapters/adapter.js";
import { IndexDependency } from "@validators/index-dependency.js";

export class Database extends Cache {
    constructor(adapter: Adapter, cache: NuvixCache, options: DatabaseOptions = {}) {
        super(adapter, cache, options);
    }

    /**
     * Creates a new database.
     */
    public async create(database?: string): Promise<void> {
        database = database ?? this.adapter.$schema;
        await this.adapter.create(database);

        const attributes = [...Database.COLLECTION.attributes]
            .map((attr) => new Doc(attr));
        await this.silent(() => this.createCollection({ id: Database.METADATA, attributes }));

        this.trigger(EventsEnum.DatabaseCreate, database);
    }

    public async exists(database?: string, collection?: string): Promise<boolean> {
        database ??= this.adapter.$schema;
        return this.adapter.exists(database, collection);
    }

    public async list(): Promise<string[]> {
        this.trigger(EventsEnum.DatabaseList, []);
        return [];
    }

    public async delete(database?: string): Promise<void> {
        database ??= this.adapter.$schema;
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
            'documentSecurity': documentSecurity ?? false,
        });

        if (this.validate) {
            const validator = new IndexValidator(
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
        if (permissions.length) {
            if (this.validate) {
                const perms = new Permissions();
                if (!perms.$valid(permissions)) {
                    throw new DatabaseException(perms.$description);
                }
            }
        }

        let collection = await this.silent(() => this.getCollection(id, true));

        if (
            this.adapter.$sharedTables
            && collection.getTenant() !== this.adapter.$tenantId
        ) {
            throw new NotFoundException(`Collection '${id}' not found`);
        }

        collection.set('$permissions', permissions);
        collection.set('documentSecurity', documentSecurity);

        // TODO: --
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

        // TODO: --
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
        // TODO: --
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

    public async createAttribute(
        collectionId: string,
        attribute: Attribute,
    ) {
        const type = attribute.type;
        if (type === AttributeEnum.Relationship || type === AttributeEnum.Virtual) {
            throw new DatabaseException(`Cannot create attribute of type '${type}'.`);
        }

        let collection = await this.silent(() => this.getCollection(collectionId, true));
        const attr = await this.validateAttribute(collection, attribute);

        collection.append('attributes', attr);

        try {
            await this.adapter.createAttribute({
                collection: collectionId,
                ...attribute,
            })
        } catch (error) {
            if (error instanceof DuplicateException) {
                // HACK: Metadata should still be updated, can be removed when null tenant collections are supported.
                if (!this.adapter.$sharedTables || !this.migrating) {
                    throw error;
                }
            }
            else throw error;
        }

        if (collection.getId() !== Database.METADATA) {
            collection = await this.silent(() => this.updateDocument(Database.METADATA, collection.getId(), collection))
        }

        this.trigger(EventsEnum.AttributeCreate, collection, attr)
        return true;
    }

    public async createAttributes(
        collectionId: string,
        attributes: Attribute[]
    ) {
        if (attributes.length === 0) {
            throw new DatabaseException('No attributes to create');
        }

        let collection = await this.silent(() => this.getCollection(collectionId, true));
        const attrDocs: Doc<Attribute>[] = []

        for (const attribute of attributes) {
            const attr = await this.validateAttribute(collection, attribute)

            collection.append('attributes', attr)
            attrDocs.push(attr);
        }

        try {
            await this.adapter.createAttributes(collection.getId(), attributes)
        } catch (error) {
            if (error instanceof DuplicateException) {
                // No attributes were in a metadata, but at least one of them was present on the table
                // HACK: Metadata should still be updated, can be removed when null tenant collections are supported.
                if (!this.adapter.$sharedTables || !this.migrating) {
                    throw error;
                }
            }
            throw error;
        }

        if (collection.getId() !== Database.METADATA) {
            collection = await this.silent(() => this.updateDocument(Database.METADATA, collection.getId(), collection))
        }

        this.purgeCachedCollection(collection);
        this.purgeCachedDocument(Database.METADATA, collection);

        this.trigger(EventsEnum.AttributesCreate, collection, attrDocs);
        return true;
    }

    /**
     * Update index metadata. Utility method for update index methods.
     */
    protected async updateIndexMeta(
        collectionId: string,
        id: string,
        updateCallback: (index: Doc<Index>, collection: Doc<Collection>, indexPosition: number) => void
    ): Promise<Doc<Index>> {
        let collection = await this.silent(() => this.getCollection(collectionId));

        if (collection.getId() === Database.METADATA) {
            throw new DatabaseException('Cannot update metadata indexes');
        }

        const indexes = collection.get('indexes', []);
        const indexPosition = indexes.findIndex((index: Doc<Index>) => index.get('$id') === id);

        if (indexPosition === -1) {
            throw new NotFoundException('Index not found');
        }

        // Execute update from callback
        updateCallback(indexes[indexPosition]!, collection, indexPosition);

        // Save
        collection.set('indexes', indexes);
        await this.silent(() => this.updateDocument(Database.METADATA, collection.getId(), collection));

        this.trigger(EventsEnum.AttributeUpdate, collection, indexes[indexPosition]!);

        return indexes[indexPosition]!;
    }

    /**
     * Update attribute metadata. Utility method for update attribute methods.
     */
    protected async updateAttributeMeta(
        collectionId: string,
        id: string,
        updateCallback: (attribute: Doc<Attribute>, collection: Doc<Collection>, index: number) => void | Promise<void>
    ): Promise<Doc<Attribute>> {
        let collection = await this.silent(() => this.getCollection(collectionId));

        if (collection.getId() === Database.METADATA) {
            throw new DatabaseException('Cannot update metadata attributes');
        }

        const attributes = collection.get('attributes', []);
        const index = attributes.findIndex((attribute: Doc<Attribute>) => attribute.get('$id') === id);

        if (index === -1) {
            throw new NotFoundException('Attribute not found');
        }

        // Execute update from callback
        const res = updateCallback(attributes[index]!, collection, index);
        if (res instanceof Promise) {
            await res;
        }

        // Save
        collection.set('attributes', attributes);
        await this.silent(() => this.updateDocument(Database.METADATA, collection.getId(), collection));

        this.trigger(EventsEnum.AttributeUpdate, collection, attributes[index]!);

        return attributes[index]!;
    }

    /**
     * Update required status of attribute.
     */
    public async updateAttributeRequired(
        collectionId: string,
        id: string,
        required: boolean
    ): Promise<Doc<Attribute>> {
        return this.updateAttributeMeta(collectionId, id, (attribute) => {
            attribute.set('required', required);
        });
    }

    /**
     * Update format of attribute.
     */
    public async updateAttributeFormat(
        collectionId: string,
        id: string,
        format: string
    ): Promise<Doc<Attribute>> {
        return this.updateAttributeMeta(collectionId, id, (attribute) => {
            if (!Structure.hasFormat(format, attribute.get('type'))) {
                throw new DatabaseException(`Format "${format}" not available for attribute type "${attribute.get('type')}"`);
            }

            attribute.set('format', format);
        });
    }

    /**
     * Update format options of attribute.
     */
    public async updateAttributeFormatOptions(
        collectionId: string,
        id: string,
        formatOptions: Record<string, any>
    ): Promise<Doc<Attribute>> {
        return this.updateAttributeMeta(collectionId, id, (attribute) => {
            attribute.set('formatOptions', formatOptions);
        });
    }

    /**
     * Update filters of attribute.
     */
    public async updateAttributeFilters(
        collectionId: string,
        id: string,
        filters: string[]
    ): Promise<Doc<Attribute>> {
        return this.updateAttributeMeta(collectionId, id, (attribute) => {
            attribute.set('filters', filters);
        });
    }

    /**
     * Update default value of attribute.
     */
    public async updateAttributeDefault(
        collectionId: string,
        id: string,
        defaultValue: any = null
    ): Promise<Doc<Attribute>> {
        return this.updateAttributeMeta(collectionId, id, (attribute) => {
            if (attribute.get('required') === true) {
                throw new DatabaseException('Cannot set a default value on a required attribute');
            }

            this.validateDefaultTypes(attribute.get('type'), defaultValue);

            attribute.set('default', defaultValue);
        });
    }

    /**
     * Update an attribute in a collection.
     */
    public async updateAttribute(
        collectionId: string,
        id: string,
        options: {
            type?: AttributeEnum;
            size?: number;
            required?: boolean;
            default?: any;
            array?: boolean;
            format?: string;
            formatOptions?: Record<string, any>;
            filters?: string[];
            newKey?: string;
        } = {}
    ): Promise<Doc<Attribute>> {
        return this.updateAttributeMeta(collectionId, id, async (attribute, collection, attributeIndex) => {
            const {
                type = attribute.get('type'),
                size = attribute.get('size'),
                required = attribute.get('required'),
                default: defaultValue = attribute.get('default'),
                array = attribute.get('array'),
                format = attribute.get('format'),
                formatOptions = attribute.get('formatOptions'),
                filters = attribute.get('filters'),
                newKey
            } = options;

            const altering = options.type !== undefined
                || options.size !== undefined
                || options.array !== undefined
                || options.newKey !== undefined;

            const finalDefault = required === true && defaultValue !== null ? null : defaultValue;

            // Validate attribute type and size constraints
            switch (type) {
                case AttributeEnum.String:
                    if (!size) {
                        throw new DatabaseException('Size length is required');
                    }
                    if (size > this.adapter.$limitForString) {
                        throw new DatabaseException(`Max size allowed for string is: ${this.adapter.$limitForString}`);
                    }
                    break;

                case AttributeEnum.Integer:
                    if (size && size > this.adapter.$limitForInt) {
                        throw new DatabaseException(`Max size allowed for int is: ${this.adapter.$limitForInt}`);
                    }
                    break;

                case AttributeEnum.Float:
                case AttributeEnum.Boolean:
                case AttributeEnum.Json:
                case AttributeEnum.Uuid:
                case AttributeEnum.Timestamptz:
                    if (size) {
                        throw new DatabaseException('Size must be empty');
                    }
                    break;
                default:
                    throw new DatabaseException(`Unknown attribute type: ${type}`);
            }

            // Validate format
            if (format && !Structure.hasFormat(format, type)) {
                throw new DatabaseException(`Format "${format}" not available for attribute type "${type}"`);
            }

            // Validate default value
            if (finalDefault !== null) {
                if (required) {
                    throw new DatabaseException('Cannot set a default value on a required attribute');
                }
                this.validateDefaultTypes(type, finalDefault);
            }

            // Update attribute properties
            const updatedId = newKey ?? id;
            attribute
                .set('$id', updatedId)
                .set('key', updatedId)
                .set('type', type)
                .set('size', size)
                .set('array', array)
                .set('format', format)
                .set('formatOptions', formatOptions)
                .set('filters', filters)
                .set('required', required)
                .set('default', finalDefault);

            const attributes = collection.get('attributes', []);
            attributes[attributeIndex] = attribute;
            collection.set('attributes', attributes);

            // Check document size limit
            if (this.adapter.$documentSizeLimit > 0 &&
                this.adapter.getAttributeWidth(collection) >= this.adapter.$documentSizeLimit) {
                throw new LimitException('Row width limit reached. Cannot update attribute.');
            }

            if (altering) {
                const indexes = collection.get('indexes', []);

                // Update index attribute references if key changed
                if (newKey && id !== newKey) {
                    indexes.forEach(index => {
                        const indexAttributes = index.get('attributes', []);
                        if (indexAttributes.includes(id)) {
                            const updatedAttributes = indexAttributes.map(attr => attr === id ? newKey : attr);
                            index.set('attributes', updatedAttributes);
                        }
                    });
                }

                // Validate indexes after attribute update
                if (this.validate) {
                    const validator = new IndexValidator(
                        attributes,
                        this.adapter.$maxIndexLength,
                        this.adapter.$internalIndexesKeys,
                        this.adapter.$supportForIndexArray
                    );

                    indexes.forEach(index => {
                        if (!validator.$valid(index)) {
                            throw new IndexException(validator.$description);
                        }
                    });
                }

                // Update attribute in adapter
                await this.adapter.updateAttribute({
                    key: id,
                    collection: collectionId,
                    type,
                    size,
                    array,
                    newName: newKey
                });
                await this.purgeCachedCollection(collection);
            }

            await this.purgeCachedDocument(Database.METADATA, collection);
        });
    }

    /**
     * Deletes an attribute from a collection.
     */
    public async deleteAttribute(collectionId: string, attributeId: string): Promise<boolean> {
        const collection = await this.silent(() => this.getCollection(collectionId));

        if (collection.getId() === Database.METADATA) {
            throw new DatabaseException('Cannot delete metadata attributes');
        }

        const attributes = collection.get('attributes', []);
        const indexes = collection.get('indexes', []);

        const attributeIndex = attributes.findIndex((attr: Doc<Attribute>) => attr.get('$id') === attributeId);
        if (attributeIndex === -1) {
            throw new NotFoundException('Attribute not found');
        }

        const attribute = attributes[attributeIndex]!;
        if (attribute.get('type') === AttributeEnum.Relationship) {
            throw new DatabaseException('Cannot delete relationship as an attribute');
        }
        if (attribute.get('type') === AttributeEnum.Virtual) {
            throw new DatabaseException('Cannot delete virtual attribute');
        }

        if (this.validate) {
            const validator = new IndexDependency(
                indexes,
                this.adapter.$supportForCastIndexArray
            );

            if (!validator.$valid(attribute)) {
                throw new DependencyException(validator.$description);
            }
        }

        // Remove attribute from indexes
        for (const index of indexes) {
            const indexAttributes = index.get('attributes', []);
            const updatedAttributes = indexAttributes.filter((attr) => attr !== attributeId);

            if (updatedAttributes.length === 0) {
                indexes.splice(indexes.indexOf(index), 1);
            } else {
                index.set('attributes', updatedAttributes);
            }
        }

        // Remove attribute from collection
        attributes.splice(attributeIndex, 1);
        collection.set('attributes', attributes);
        collection.set('indexes', indexes);

        try {
            await this.adapter.deleteAttribute(collection.getId(), attributeId);
        } catch (error) {
            if (!(error instanceof NotFoundException)) {
                throw error;
            }
        }

        if (collection.getId() !== Database.METADATA) {
            await this.silent(() => this.updateDocument(Database.METADATA, collection.getId(), collection));
        }

        await this.purgeCachedCollection(collection);
        await this.purgeCachedDocument(Database.METADATA, collection);

        this.trigger(EventsEnum.AttributeDelete, collection, attribute);

        return true;
    }



    public getDocument<C extends (string & keyof Entities)>(
        collectionId: C,
        id: string,
        query?: ((builder: QueryBuilder) => QueryBuilder) | Query[],
        forUpdate?: boolean,
    ): Promise<Doc<Entities[C]>>;
    public getDocument<C extends string>(
        collectionId: C,
        id: string,
        query?: ((builder: QueryBuilder) => QueryBuilder) | Query[],
        forUpdate?: boolean,
    ): Promise<Doc<Partial<IEntity> & Record<string, any>>>;
    public getDocument<D extends Record<string, any>>(
        collectionId: string,
        id: string,
        query?: ((builder: QueryBuilder) => QueryBuilder) | Query[],
        forUpdate?: boolean,
    ): Promise<Doc<Partial<IEntity> & D>>;
    public async getDocument(
        collectionId: string,
        id: string,
        query: ((builder: QueryBuilder) => QueryBuilder) | Query[] = [],
        forUpdate: boolean = false,
    ): Promise<any> {
        if (collectionId === Database.METADATA && id === Database.METADATA) {
            return new Doc(Database.COLLECTION) as any;
        }

        if (!collectionId) {
            throw new NotFoundException(`Collection '${collectionId}' not found.`);
        }
        if (!id) return new Doc();

        const collection = await this.silent(() => this.getCollection(collectionId, true));
        const validator = new Authorization(PermissionEnum.Read);

        if (collection.getId() !== Database.METADATA) {
            if (!validator.$valid([
                ...collection.getRead(),
            ])) {
                return new Doc();
            }
        }

        const processedQuery = await this.processQueries(query, collection);
        const documentSecurity = collection.get('documentSecurity', false);

        let doc = await this.adapter.getDocument(collection.getId(), id, processedQuery);

        if (doc.empty()) {
            return new Doc();
        }

        doc = this.cast(collection, doc);
        doc = await this.decode(collection, doc);

        this.trigger(EventsEnum.DocumentRead, doc)
        return doc;
    }

    public async createDocument<C extends (string & keyof Entities) | Partial<IEntity> & Record<string, any>>(
        collectionId: C extends string ? C : string,
        document: C extends string ? Partial<Entities[C]> | Doc<Partial<Entities[C]>> : C extends Record<string, any> ? Partial<C & IEntity> | Doc<Partial<C & IEntity>> : Partial<IEntity> | Doc<IEntity>
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

        const collection = await this.silent(() => this.getCollection(collectionId, true));

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

        doc.set('$id', doc.getId() || ID.unique())
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

        const castedResult = this.cast(collection, result);
        const decodedResult = await this.decode(collection, castedResult);

        this.trigger(EventsEnum.DocumentCreate, decodedResult);

        return decodedResult as any;
    }


    public async updateDocument<C extends (string & keyof Entities)>(
        collectionId: C,
        id: string,
        document: Entities[C]
    ): Promise<Doc<Entities[C]>>;
    public async updateDocument<D extends Record<string, any>>(
        collectionId: string,
        id: string,
        document: D
    ): Promise<D>;

    public async updateDocument(
        collectionId: string,
        id: string,
        document: any
    ): Promise<Doc<any>> {
        // TODO: Implement update logic here
        return new Doc(document);
    }

    async processQueries(
        queries: ((builder: QueryBuilder) => QueryBuilder) | Query[],
        collection: Doc<Collection>,
        metadata: Partial<Attribute['options']> & {
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

        let { populate: _populate, selections } = Query.groupByType(queries);
        const attributes = collection.get('attributes', []);

        if (selections.length > 0) {
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
        } else {
            selections = [Query.select(attributes.filter(a => a.get('type') !== AttributeEnum.Relationship && a.get('type') !== AttributeEnum.Virtual).map(a => a.get('key', a.getId())))];
        }

        if (!_populate.size) {
            return {
                queries,
                collection,
                selections: selections.map(q => q.getValues() as unknown as string[]).flat(),
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

            const relatedCollectionId = attributeDoc.get('options', {} as any)['relatedCollection'];
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
            selections: selections.map(q => q.getValues() as unknown as string[]).flat(),
            populate
        };
    }
}

export interface ProcessQuery {
    queries: Query[];
    collection: Doc<Collection>;
    selections: string[];
    populate?: ProcessQuery[]
}

export type DatabaseOptions = {
    tenant?: number;
    filters?: Filters;
};
