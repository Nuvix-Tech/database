import { DatabaseException } from "@errors/base.js";
import { Permission } from "@utils/permission.js";
import { IEntity, IEntityInput } from "types.js";
import chalk from "chalk";

type IsReferenceObject<T> = T extends { $id: string }
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

type Simplify<T> = { [K in keyof T]: T[K] };

type FilterInput<T> = Partial<Omit<T, "$permissions">> & IEntityInput;

function isEntityLike(value: unknown): value is Record<string, unknown> {
  return (
    typeof value === "object" &&
    value !== null &&
    ("$id" in value || "$collection" in value)
  );
}

export class Doc<
  T extends Record<string, any> & Partial<IEntity> = Partial<IEntity>,
> {
  private _data: Record<string, any> = {};

  constructor(data: T extends IEntity ? FilterInput<T> : never);
  constructor(
    data?: (T | TransformEntity<T>) | (IEntityInput & Record<string, any>),
  );
  constructor(
    data?: (T | TransformEntity<T>) | (IEntityInput & Record<string, any>),
  ) {
    this._data = {};
    if (data) {
      if (data.$id && typeof data.$id !== "string") {
        throw new DocException("$id must be a string");
      }

      if (data.$permissions && !Array.isArray(data.$permissions)) {
        throw new DocException("$permissions must be an array");
      }

      for (const [key, value] of Object.entries(data)) {
        if (Array.isArray(value)) {
          this._data[key] = value.map((item) =>
            isEntityLike(item)
              ? item instanceof Doc
                ? item
                : new Doc(item as any)
              : item,
          );
        } else if (isEntityLike(value)) {
          this._data[key] =
            value instanceof Doc ? value : new Doc(value as any);
        } else {
          this._data[key] = value ?? null;
        }
      }
    }
  }

  static from<D extends Partial<IEntity>>(data: D & IEntityInput): Doc<D> {
    return new Doc(data);
  }

  public get<K extends keyof T>(
    name: K,
  ): Exclude<TransformEntity<T>[K], undefined>;
  public get<K extends keyof T, D extends T[K]>(
    name: K,
    _default?: D,
  ): Exclude<TransformEntity<T>[K], undefined>;
  public get<K extends keyof T, D = null>(
    name: K,
    _default?: D,
  ): Exclude<TransformEntity<T>[K], undefined> | D;
  public get<K extends string, D = null>(name: K, _default?: D): D;
  public get<K extends keyof T, D = null>(
    name: K,
    _default: D = null as D,
  ): Exclude<TransformEntity<T>[K], undefined> | D {
    const value = this._data[name as string];
    return value === undefined ? _default : value;
  }

  public getAll(): TransformEntity<T> {
    return { ...this._data } as TransformEntity<T>;
  }

  public set<K extends keyof T>(name: K, value: TransformField<T[K]>): this;
  public set<K extends string, V extends unknown>(
    name: K,
    value: V,
  ): Doc<Simplify<T & Record<K, TransformField<V>>>>;
  public set<K extends string, V extends unknown>(name: K, value: V): any {
    if (Array.isArray(value)) {
      this._data[name] = value.map((item) =>
        isEntityLike(item)
          ? item instanceof Doc
            ? item
            : new Doc(item as any)
          : item,
      );
    } else if (isEntityLike(value)) {
      this._data[name] = value instanceof Doc ? value : new Doc(value as any);
    } else {
      this._data[name] = value ?? null;
    }
    return this;
  }

  public setAll(data: FilterInput<T>): this;
  public setAll<D extends FilterInput<T> & Record<string, any>>(
    data: D,
  ): Doc<Simplify<T & D>>;
  public setAll(data: FilterInput<T>): this {
    for (const [key, value] of Object.entries(data)) {
      if (Array.isArray(value)) {
        this._data[key] = value.map((item) =>
          isEntityLike(item)
            ? item instanceof Doc
              ? item
              : new Doc(item as any)
            : item,
        );
      } else if (isEntityLike(value)) {
        this._data[key] =
          (value as any) instanceof Doc ? value : new Doc(value as any);
      } else {
        this._data[key] = value ?? null;
      }
    }
    return this;
  }

  public append<K extends string & keyof T>(
    name: K,
    value: TransformField<T[K]> extends Array<unknown>
      ? TransformField<T[K]>[number]
      : TransformField<T[K][number]>,
  ): this {
    if (!Array.isArray(this._data[name])) {
      throw new DocException(
        `Cannot append to ${String(name)}, it is not an array`,
      );
    }
    if (isEntityLike(value)) {
      this._data[name].push(
        value instanceof Doc ? value : new Doc(value as any),
      );
    } else {
      this._data[name].push(value);
    }
    return this;
  }

  public prepend<K extends string & keyof T>(
    name: K,
    value: TransformField<T[K]> extends Array<any>
      ? TransformField<T[K]>[number]
      : TransformField<T[K]>,
  ): this {
    if (!Array.isArray(this._data[name])) {
      throw new DocException(
        `Cannot prepend to ${String(name)}, it is not an array`,
      );
    }
    if (isEntityLike(value)) {
      this._data[name].unshift(
        value instanceof Doc ? value : new Doc(value as any),
      );
    } else {
      this._data[name].unshift(value);
    }
    return this;
  }

  public delete<K extends string & keyof T>(name: K): this {
    if (name in this._data) {
      delete this._data[name];
    }
    return this;
  }

  public getId(): string {
    return this.get("$id") as string;
  }

  public getSequence(): number {
    return this.get("$sequence") as number;
  }

  public getTenant(): number | null {
    const tenant = this.get("$tenant", null);
    if (tenant === null || typeof tenant === "number") {
      return tenant;
    } else {
      throw new DocException("$tenant must be a number or null");
    }
  }

  public getCollection(): string {
    const collection = this.get("$collection");
    // if (typeof collection !== "string") {
    //     throw new DocException("$collection must be a string");
    // }
    return collection as string;
  }

  public createdAt(): Date | null {
    const value = this.get("$createdAt", null);
    if (typeof value === "string") {
      return new Date(value);
    }
    return value as Date | null;
  }

  public updatedAt(): Date | null {
    const value = this.get("$updatedAt", null);
    if (typeof value === "string") {
      return new Date(value);
    }
    return value as Date | null;
  }

  public getPermissions(): string[] {
    const permissions: (string | Permission)[] = this.get(
      "$permissions",
      [],
    ) as any;

    return Array.from(
      new Set(
        permissions
          .map((p) => (p instanceof Permission ? p.toString() : p))
          .filter(Boolean),
      ),
    );
  }

  public getRead(): string[] {
    return this.getPermissionsByType("read");
  }

  public getCreate(): string[] {
    return this.getPermissionsByType("create");
  }

  public getUpdate(): string[] {
    return this.getPermissionsByType("update");
  }

  public getDelete(): string[] {
    return this.getPermissionsByType("delete");
  }

  public getWrite() {
    return Array.from(
      new Set([...this.getCreate(), ...this.getUpdate(), ...this.getDelete()]),
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

  public has(name: keyof T): boolean;
  public has(name: string): boolean;
  public has(name: string): boolean {
    return Object.hasOwn(this._data, name);
  }

  public keys(): (keyof T)[] {
    return Object.keys(this._data);
  }

  public findWhere<K extends string & keyof T>(
    key: K,
    predicate: (item: T[K] extends Array<any> ? T[K][number] : T[K]) => boolean,
  ): T[K] extends Array<any> ? T[K][number] : T[K] | null;
  public findWhere<V = unknown>(
    key: string,
    predicate: (item: V) => boolean,
  ): V | null;
  public findWhere<V = unknown>(
    key: string & keyof T,
    predicate: (item: V) => boolean,
  ): V | null {
    // Recursively search for a value matching the predicate at the given key in this entity and all nested entities/arrays
    const value = this.get(key);
    if (Array.isArray(value)) {
      for (const item of value as unknown[]) {
        if (item !== undefined && predicate(item as V)) {
          return item as V;
        }
      }
    } else if (value !== undefined && predicate(value as V)) {
      return value as V;
    }
    return null;
  }

  public replaceWhere<V = unknown>(
    key: string & keyof T,
    predicate: (item: V) => boolean,
    replacement: V | ((item: V) => V),
  ): void;
  public replaceWhere<V = unknown>(
    key: string,
    predicate: (item: V) => boolean,
    replacement: V | ((item: V) => V),
  ): void;
  public replaceWhere<V = unknown>(
    key: string & keyof T,
    predicate: (item: V) => boolean,
    replacement: V | ((item: V) => V),
  ): void {
    const value = this.get(key);
    if (Array.isArray(value)) {
      for (let i = 0; i < value.length; i++) {
        if (predicate(value[i] as V)) {
          if (typeof replacement === "function") {
            value[i] = (replacement as (item: V) => V)(value[i] as V);
          } else {
            value[i] = replacement;
          }
        }
      }
    } else if (value !== undefined && predicate(value as V)) {
      if (typeof replacement === "function") {
        this.set(key, (replacement as (item: V) => V)(value as V));
      } else {
        this.set(key, replacement);
      }
    }
  }

  public deleteWhere<V = unknown>(
    key: string & keyof T,
    predicate: (item: V) => boolean,
  ): void;
  public deleteWhere<V = unknown>(
    key: string,
    predicate: (item: V) => boolean,
  ): void;
  public deleteWhere<V = unknown>(
    key: string & keyof T,
    predicate: (item: V) => boolean,
  ): void {
    const value = this.get(key);
    if (Array.isArray(value)) {
      this.set(
        key,
        value.filter((item: V) => !predicate(item)),
      );
    } else if (value !== undefined && predicate(value as V)) {
      this.delete(key);
    }
  }

  public empty(): boolean {
    return this.keys().length === 0;
  }

  public toObject(): T;
  public toObject(allow: (keyof T)[], disallow?: (keyof T)[]): T;
  public toObject(allow: any[] = [], disallow: any[] = []): T {
    const output: Record<string, unknown> = {};
    const keys = this.keys();
    for (const key of keys) {
      const value = this._data[key as string];
      if (allow.length && !allow.includes(key)) continue;
      if (disallow.includes(key)) continue;

      if (value instanceof Doc) {
        output[key as string] = value.toObject(allow, disallow);
      } else if (Array.isArray(value)) {
        output[key as string] = value.map((item) =>
          item instanceof Doc ? item.toObject(allow, disallow) : item,
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
      const value = this._data[key as string];
      if (value instanceof Doc) {
        (cloned as any)._data[key as string] = value.clone();
      } else if (Array.isArray(value)) {
        (cloned as any)._data[key as string] = value.map((item) =>
          item instanceof Doc ? item.clone() : item,
        );
      } else {
        (cloned as any)._data[key as string] = value;
      }
    }
    return cloned;
  }

  [Symbol.for("nodejs.util.inspect.custom")]() {
    const formatValue = (value: any, depth: number = 0): string => {
      if (value instanceof Doc) {
        return chalk.cyan(`Doc(${formatValue(value._data, depth + 1)})`);
      } else if (Array.isArray(value)) {
        return chalk.green(
          `[${value.map((item) => formatValue(item, depth + 1)).join(", ")}]`,
        );
      } else if (typeof value === "object" && value !== null) {
        const indent = "  ".repeat(depth + 1);
        const entries = Object.entries(value)
          .map(
            ([key, val]) =>
              `${indent}${chalk.yellow(key)}: ${formatValue(val, depth + 1)}`,
          )
          .join(",\n");
        return `{\n${entries}\n${"  ".repeat(depth)}}`;
      } else if (typeof value === "string") {
        return chalk.magenta(`"${value}"`);
      } else if (typeof value === "number") {
        return chalk.blue(value.toString());
      } else if (typeof value === "boolean") {
        return chalk.red(value.toString());
      } else if (value === null) {
        return chalk.gray("null");
      } else {
        return String(value);
      }
    };

    return `Doc ${formatValue(this._data)}`;
  }
}

export class DocException extends DatabaseException {}
