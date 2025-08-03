import { StructureException } from "errors/index.js";
import { IEntity, IEntityInput } from "types.js";

type IsReferenceObject<T> =
    T extends { $id: string }
    ? true
    : T extends { $collection: string }
    ? true
    : false;

type TransformField<T> =
    IsReferenceObject<T> extends true
    ? Doc<T extends Partial<IEntity> ? T : Partial<IEntity>>
    : T extends Array<infer U>
    ? Array<TransformField<U>>
    : T extends object
    ? TransformEntity<T>
    : T;

type TransformEntity<T> = {
    [K in keyof T]: TransformField<T[K]>;
};

function isEntityLike(value: unknown): value is Record<string, unknown> {
    return (
        typeof value === "object" &&
        value !== null &&
        ("$id" in value || "$collection" in value)
    );
}

export class Doc<T extends Record<string, any> & Partial<IEntity> = IEntity> {
    private static readonly __methods_keys: string[] = [
        "__methods_keys",
        "get",
        "set",
        "getAll",
        "append",
        "prepend",
        "getId",
        "getSequence",
        "getTenant",
        "getInternalId",
        "getCollection",
        "createdAt",
        "updatedAt",
        "getPermissions",
        "getRead",
        "getCreate",
        "getUpdate",
        "getDelete",
        "getWrite",
        "getPermissionsByType",
        "keys",
        "has",
        "empty",
        "findWhere",
        "replaceWhere",
        "deleteWhere",
        "toObject",
        "toJSON",
        "clone",
        "toString",
    ];

    constructor(data?: (T | TransformEntity<T>) | IEntityInput) {
        if (data) {
            if (data.$id && typeof data.$id !== "string") {
                throw new StructureException("$id must be a string");
            }

            if (data.$permissions && !Array.isArray(data.$permissions)) {
                throw new StructureException("$permissions must be an array");
            }

            for (const [key, value] of Object.entries(data)) {
                if (Array.isArray(value)) {
                    (this as any)[key] = value.map((item) =>
                        isEntityLike(item) ? new Doc(item as any) : item
                    );
                } else if (isEntityLike(value)) {
                    (this as any)[key] = new Doc(value as any);
                } else {
                    (this as any)[key] = value ?? null;
                }
            }
        }
    }

    static from<D extends Partial<IEntity>>(data: D & IEntityInput): Doc<D> {
        return new Doc(data);
    }

    public get<K extends keyof T, D = null>(name: K, _default?: D): Exclude<TransformEntity<T>[K], undefined> | D;
    public get<K extends string, D = null>(name: K, _default?: D): D;
    public get<K extends keyof T, D = null>(name: K, _default: D = null as D): Exclude<TransformEntity<T>[K], undefined> | D {
        const value = (this as any)[name];
        return value === undefined ? _default : value;
    }

    public getAll(): TransformEntity<T> {
        const output: Partial<TransformEntity<T>> = {};
        const keys = this.keys();
        for (const key of keys) {
            output[key] = (this as any)[key];
        }
        return output as TransformEntity<T>;
    }

    public set<K extends keyof T>(name: K, value: TransformField<T[K]>): this;
    public set<K extends string, V extends unknown>(name: K, value: V): Doc<T & Record<K, TransformField<V>>>;
    public set<K extends keyof T>(name: K, value: TransformField<T[K]>): this | Doc<T & Record<K, TransformField<T[K]>>> {
        if (isEntityLike(value)) {
            (this as any)[name] = value instanceof Doc ? value : new Doc(value as any);
        } else {
            (this as any)[name] = value;
        }
        return this;
    }

    public append<K extends keyof T>(name: K, value: TransformField<T[K]>): this {
        if (!Array.isArray((this as any)[name])) {
            throw new StructureException(`Cannot append to ${String(name)}, it is not an array`);
        }
        if (isEntityLike(value)) {
            (this as any)[name].push(value instanceof Doc ? value : new Doc(value as any));
        } else {
            (this as any)[name].push(value);
        }
        return this;
    }

    public prepend<K extends keyof T>(name: K, value: TransformField<T[K]>): this {
        if (!Array.isArray((this as any)[name])) {
            throw new StructureException(`Cannot prepend to ${String(name)}, it is not an array`);
        }
        if (isEntityLike(value)) {
            (this as any)[name].unshift(value instanceof Doc ? value : new Doc(value as any));
        } else {
            (this as any)[name].unshift(value);
        }
        return this;
    }

    public delete<K extends keyof T>(name: K): this {
        if (name in this) {
            delete (this as any)[name];
        } else {
            throw new StructureException(`Property ${String(name)} does not exist on this entity`);
        }
        return this;
    }

    public getId(): string {
        return this.get('$id') as string;
    }

    public getSequence(): number {
        return this.get("$sequence") as number;
    }

    public getTenant(): number | null {
        const tenant = this.get("$tenant", null);
        if (tenant === null || typeof tenant === "number") {
            return tenant;
        } else {
            throw new StructureException("$tenant must be a number or null");
        }
    }

    /**
     * @deprecated use getSequence instead
     */
    public getInternalId(): number {
        console.warn("getInternalId is deprecated, use getSequence instead");
        return this.getSequence();
    }

    public getCollection(): string {
        const collection = this.get("$collection");
        if (typeof collection !== "string") {
            throw new StructureException("$collection must be a string");
        }
        return collection;
    }

    public createdAt(): Date | null {
        const value = this.get('$createdAt', null);
        if (typeof value === 'string') {
            return new Date(value);
        }
        return value as Date | null;
    }

    public updatedAt(): Date | null {
        const value = this.get('$updatedAt', null);
        if (typeof value === 'string') {
            return new Date(value);
        }
        return value as Date | null;
    }

    public getPermissions(): string[] {
        return Array.from(
            new Set(this.get("$permissions", []) as string[]),
        );
    }

    public getRead(): `read:${string}`[] {
        return this.getPermissionsByType("read") as `read:${string}`[];
    }

    public getCreate(): `create:${string}`[] {
        return this.getPermissionsByType("create") as `create:${string}`[];
    }

    public getUpdate(): `update:${string}`[] {
        return this.getPermissionsByType("update") as `update:${string}`[];
    }

    public getDelete(): `delete:${string}`[] {
        return this.getPermissionsByType("delete") as `delete:${string}`[];
    }

    public getWrite() {
        return Array.from(
            new Set([
                ...this.getCreate(),
                ...this.getUpdate(),
                ...this.getDelete(),
            ]),
        );
    }

    public getPermissionsByType(type: string): string[] {
        return this.getPermissions()
            .filter((permission) => permission.startsWith(type))
            .map((permission) =>
                permission
                    .replace(`${type}(`, "")
                    .replace(")", "")
                    .replace(/"/g, "")
                    .trim(),
            );
    }

    public has<K extends keyof T>(name: K): boolean {
        return (this as any)[name] !== undefined;
    }

    public keys(): (keyof T)[] {
        const keys = Object.keys(this);
        return keys.filter(
            (key) => !Doc.__methods_keys.includes(key),
        ) as (keyof T)[];
    }

    public findWhere<V = unknown>(
        key: keyof T,
        predicate: (item: V) => boolean,
    ): V | null {
        // Recursively search for a value matching the predicate at the given key in this entity and all nested entities/arrays
        const value = this.get(key);
        if (Array.isArray(value)) {
            for (const item of value as unknown[]) {
                if (item instanceof Doc) {
                    const found = item.findWhere(key, predicate);
                    if (found !== null) {
                        return found;
                    }
                } else if (item !== undefined && predicate(item as V)) {
                    return item as V;
                }
            }
        } else if (value instanceof Doc) {
            const found = value.findWhere(key, predicate);
            if (found !== null) {
                return found;
            }
        } else if (value !== undefined && predicate(value as V)) {
            return value as V;
        }

        // Recursively search all fields for nested entities/arrays
        for (const k of this.keys()) {
            if (k === key) continue;
            const field = (this as any)[k];
            if (field instanceof Doc) {
                const found = field.findWhere(key, predicate);
                if (found !== null) {
                    return found;
                }
            } else if (Array.isArray(field)) {
                for (const item of field) {
                    if (item instanceof Doc) {
                        const found = item.findWhere(key, predicate);
                        if (found !== null) {
                            return found;
                        }
                    }
                }
            }
        }
        return null;
    }

    public replaceWhere<V = unknown>(
        key: keyof T,
        predicate: (item: V) => boolean,
        replacement: V,
    ): void {
        // Recursively replace values matching the predicate at the given key in this entity and all nested entities/arrays
        const value = this.get(key);
        if (Array.isArray(value)) {
            for (let i = 0; i < value.length; i++) {
                if (value[i] instanceof Doc) {
                    value[i].replaceWhere(key, predicate, replacement);
                } else if (value[i] !== undefined && predicate(value[i] as V)) {
                    value[i] = replacement;
                }
            }
        } else if (value instanceof Doc) {
            value.replaceWhere(key, predicate, replacement);
        } else if (value !== undefined && predicate(value as V)) {
            (this as any)[key] = replacement;
        }

        // Recursively replace in all fields for nested entities/arrays
        for (const k of this.keys()) {
            if (k === key) continue;
            const field = (this as any)[k];
            if (field instanceof Doc) {
                field.replaceWhere(key, predicate, replacement);
            } else if (Array.isArray(field)) {
                for (const item of field) {
                    if (item instanceof Doc) {
                        item.replaceWhere(key, predicate, replacement);
                    }
                }
            }
        }
    }

    public deleteWhere<V = unknown>(
        key: keyof T,
        predicate: (item: V) => boolean,
    ): void {
        // Recursively delete values matching the predicate at the given key in this entity and all nested entities/arrays
        const value = this.get(key);
        if (Array.isArray(value)) {
            for (let i = value.length - 1; i >= 0; i--) {
                if (value[i] instanceof Doc) {
                    value[i].deleteWhere(key, predicate);
                } else if (value[i] !== undefined && predicate(value[i] as V)) {
                    value.splice(i, 1);
                }
            }
        } else if (value instanceof Doc) {
            value.deleteWhere(key, predicate);
        } else if (value !== undefined && predicate(value as V)) {
            this.delete(key);
        }

        // Recursively delete in all fields for nested entities/arrays
        for (const k of this.keys()) {
            if (k === key) continue;
            const field = (this as any)[k];
            if (field instanceof Doc) {
                field.deleteWhere(key, predicate);
            } else if (Array.isArray(field)) {
                for (const item of field) {
                    if (item instanceof Doc) {
                        item.deleteWhere(key, predicate);
                    }
                }
            }
        }
    }

    public empty(): boolean {
        return this.keys().length === 0;
    }

    public toObject(): T;
    public toObject(
        allow: (keyof T)[],
        disallow?: (keyof T)[],
    ): T;
    public toObject(
        allow: any[] = [],
        disallow: any[] = [],
    ): T {
        const output: Record<string, unknown> = {};
        const keys = this.keys();
        for (const key of keys) {
            const value = (this as any)[key];
            if (allow.length && !allow.includes(key)) continue;
            if (disallow.includes(key)) continue;

            if (value instanceof Doc) {
                output[key as string] = value.toObject(allow, disallow);
            } else if (Array.isArray(value)) {
                output[key as string] = value.map((item) =>
                    item instanceof Doc
                        ? item.toObject(allow, disallow)
                        : item,
                );
            } else {
                output[key as string] = value;
            }
        }
        return output as T;
    }

    toJSON() {
        return this.toObject();
    }

    clone() {
        const cloned = new Doc<T>();
        const keys = this.keys();
        for (const key of keys) {
            const value = (this as any)[key];
            if (value instanceof Doc) {
                (cloned as any)[key] = value.clone();
            } else if (Array.isArray(value)) {
                (cloned as any)[key] = value.map((item) =>
                    item instanceof Doc ? item.clone() : item,
                );
            } else {
                (cloned as any)[key] = value;
            }
        }
        return cloned;
    }
}
