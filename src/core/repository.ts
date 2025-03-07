import Permission from "../security/Permission";
import { Database } from "./database";
import { DeepPartial } from "./types/deep-partial";
import { Logger } from "./logger";
import { DatabaseError } from "../errors/base";
import { Document } from "./Document";
import { Query } from "./query";

interface BaseEntity {
    $id: string;

    $createdAt?: Date;

    $updatedAt?: Date;

    $permissions?: string[] | Permission[];

    $tenant?: number | null;

    $internalId: number | null;
}

/**
 * A repository for a specific entity type.
 */
export class Repository<Entity extends { [key: string]: any }> {
    readonly target: Entity;

    readonly database?: Database;

    private readonly logger: Logger;

    private metadata: Document;

    constructor(target: Entity, database: Database, logger?: Logger) {
        this.database = database;
        this.logger = logger ?? new Logger();
        this.metadata = this.getMetaData(target);
        this.target = target;
    }

    /**
     * Creates a new entity instance.
     */
    create(): Entity & BaseEntity;

    /**
     * Creates new entities and copies all entity properties from given objects into their new entities.
     */
    create(
        entityLikeArray: DeepPartial<Entity & BaseEntity>[],
    ): Array<Entity & BaseEntity>;

    /**
     * Creates a new entity instance and copies all entity properties from this object into a new entity.
     */
    create(entityLike: DeepPartial<Entity & BaseEntity>): Entity & BaseEntity;

    /**
     * Implementation of the create method.
     */
    create(
        plainEntityLikeOrPlainEntityLikes?:
            | DeepPartial<Entity & BaseEntity>
            | DeepPartial<Entity & BaseEntity>[],
    ): (Entity & BaseEntity) | Array<Entity & BaseEntity> {
        if (Array.isArray(plainEntityLikeOrPlainEntityLikes)) {
            return plainEntityLikeOrPlainEntityLikes.map(
                (entityLike) => this.create(entityLike) as any,
            );
        }
        const entity = { ...this.target } as Entity & BaseEntity;
        Object.assign(entity, plainEntityLikeOrPlainEntityLikes);
        return entity;
    }

    /**
     * Saves an entity or array of entities to the database.
     */
    async save(entity: Entity | Entity[]): Promise<void> {
        if (!this.database) {
            throw new Error("Database connection is not initialized.");
        }

        const entities = Array.isArray(entity) ? entity : [entity];
        for (const e of entities) {
            let doc = await this.database.createDocument(
                this.metadata.getId(),
                new Document(e),
            );
            Object.assign(e, doc.toObject());
        }
    }

    /**
     * Finds all entities in the repository.
     */
    async find(options?: {
        where?: Partial<Entity & BaseEntity>;
    }): Promise<Entity[]> {
        if (!this.database) {
            throw new Error("Database connection is not initialized.");
        }

        const queries: Query[] = Object.entries(options?.where ?? {}).map(
            ([key, value]) => Query.equal(key, [value]),
        );

        return (await this.database.find(
            this.metadata.getId(),
            queries,
        )) as any;
    }

    /**
     * Finds a single entity by ID.
     */
    async findOne(
        where?: Partial<Entity & BaseEntity>,
    ): Promise<Entity | null> {
        if (!this.database) {
            throw new Error("Database connection is not initialized.");
        }

        const queries: Query[] = Object.entries(where ?? {}).map(
            ([key, value]) => Query.equal(key, [value]),
        );

        return this.database.findOne(this.metadata.getId(), queries) as any;
    }

    /**
     * Deletes an entity by ID or instance.
     */
    async delete(entityOrId: Entity | number | string): Promise<void> {
        if (!this.database) {
            throw new Error("Database connection is not initialized.");
        }

        const id =
            typeof entityOrId === "object" ? entityOrId["$id"] : entityOrId;
        await this.database.deleteDocument(this.metadata.getId(), id);
    }

    private getMetaData(target: Entity): Document {
        const keys = Reflect.getMetadataKeys(target);

        if (!keys.includes("entity") || !keys.includes("columns")) {
            throw new DatabaseError(
                `Entity metadata is not valid for target: ${target.constructor.name}`,
            );
        }

        const attributes = Reflect.getMetadata("columns", target) || [];
        const indexes = Reflect.getMetadata("indexes", target) || [];
        const _meta = Reflect.getMetadata("entity", target) || {};

        return new Document({
            $id: this.database
                ?.getAdapter()
                .filter(_meta.name || target.constructor.name) as string,
            $permissions: _meta?.$permissions,
            attributes,
            indexes: indexes,
            documentSecurity: _meta.documentSecurity || false,
        });
    }

    public getMeta(): Document {
        return this.metadata;
    }
}
