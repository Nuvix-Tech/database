import { AttributeEnum, EventsEnum, PermissionEnum } from "./enums.js";
import { Attribute, Collection } from "@validators/schema.js";
import { Adapter } from "@adapters/base.js";
import { CreateCollection, Filters } from "./types.js";
import { Cache } from "./cache.js";
import { Cache as NuvixCache } from '@nuvix/cache';
import { Entities, IEntity } from "types.js";
import { QueryBuilder } from "@utils/query-builder.js";
import { Query } from "./query.js";
import { Doc } from "./doc.js";
import { DatabaseException, DuplicateException, IndexException, LimitException, NotFoundException } from "@errors/index.js";
import { Permission } from "@utils/permission.js";
import { Role } from "@utils/role.js";
import { Permissions } from "@validators/permissions.js";
import { Index } from "@validators/index-validator.js";

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
            const perms = new Permissions()
            if (!perms.$valid(permissions)) throw new DatabaseException(perms.$description);
        }

        let collection = await this.silent(() => this.getCollection(id));
        if (!collection.empty() && id !== Database.METADATA) {
            throw new DuplicateException(`Collection "${id}" already exists.`);
        }

        // Fix metadata index length & orders
        for (let i = 0; i < indexes.length; i++) {
            const index = indexes[i]!;
            const lengths: (number | null)[] = index.get('lengths', []);
            const orders: (string | null)[] = index.get('orders', []);

            const indexAttributes = index.get('attributes', []);
            for (let j = 0; j < indexAttributes.length; j++) {
                const attr = indexAttributes[j];
                for (const collectionAttribute of attributes) {
                    if (collectionAttribute.get('$id') === attr) {
                        // mysql does not save length in collection when length = attributes size
                        if (collectionAttribute.get('type') === AttributeEnum.String) {
                            if (lengths[j] && lengths[j] === collectionAttribute.get('size') && this.adapter.$maxIndexLength > 0) {
                                lengths[j] = null;
                            }
                        }

                        const isArray = collectionAttribute.get('array', false);
                        if (isArray) {
                            if (this.adapter.$maxIndexLength > 0) {
                                lengths[j] = Database.ARRAY_INDEX_LENGTH;
                            }
                            orders[j] = null;
                        }
                        break;
                    }
                }
            }

            index
                .set('lengths', lengths)
                .set('orders', orders);
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
            )
            indexes.forEach((index) => {
                if (!validator.$valid(index)) throw new IndexException(validator.$description);
            })
        }

        if (indexes.length && this.adapter.getCountOfIndexes(collection) > this.adapter.$limitForIndexes) {
            throw new LimitException(`Index limit of ${this.adapter.$limitForIndexes} exceeded. Cannot create collection.`);
        }

        if (attributes.length) {
            if (this.adapter.$limitForAttributes && attributes.length > this.adapter.$limitForAttributes) {
                throw new LimitException(`Attribute limit of ${this.adapter.$limitForAttributes} exceeded. Cannot create collection.`,);
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

    /**
     * Retrieves a collection by its ID.
     * If the collection is not found or does not match the tenant ID, an empty Doc
     */
    public async getCollection(id: string): Promise<Doc<Collection>> {
        const collection = await this.silent(() => this.getDocument<Collection>(Database.METADATA, id));

        if (id !== Database.METADATA
            && this.adapter.$sharedTables
            && collection.getTenant() !== null
            && collection.getTenant() !== this.adapter.$tenantId
        ) {
            return new Doc<Collection>();
        }

        this.trigger(EventsEnum.CollectionRead, collection);

        return collection;
    }

    /**
     * Lists all collections in the database.
     */
    public async listCollections(
        limit: number = 25,
        offset: number = 0,
    ): Promise<Doc<Collection>[]> {
        throw new Error("Method not implemented.");
    }


    public async getDocument<C extends (string & keyof Entities) | Partial<IEntity> & Record<string, any>>(
        collection: C extends string ? C : string,
        id: string,
        queries: ((builder: QueryBuilder) => QueryBuilder) | Query[] = [],
        forUpdate: boolean = false,
    ): Promise<C extends string ? Doc<Entities[C]> : Doc<C>> {
        if (collection === Database.METADATA && id === Database.METADATA) {
            return new Doc(Database.COLLECTION) as any;
        }

        if (!collection) throw new NotFoundException(`Collection "${collection}" not found.`);
        if (!id) return new Doc() as any;

        const $collection = await this.silent(() => this.getCollection(collection));

    }

}


export type DatabaseOptions = {
    tenant?: number;
    filters?: Filters;
};
