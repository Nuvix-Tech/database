import { DatabaseError } from "../errors/base";
import { Constant as Database } from "./constant";

export interface IDocument {
    $permissions: string[];
    $createdAt: string | null;
    $updatedAt: string | null;
    $id: string;
    $internalId: string;
    $collection: string;

    [key: string]: unknown | null;
}

export class Document<
    T extends Record<string, unknown> = IDocument & any,
> extends Map<keyof (IDocument & T), (IDocument & T)[keyof (IDocument & T)]> {
    public static readonly SET_TYPE_ASSIGN = "assign";
    public static readonly SET_TYPE_PREPEND = "prepend";
    public static readonly SET_TYPE_APPEND = "append";

    /**
     * Construct a new Document object
     *
     * @param input - Initial data for the document
     * @throws DatabaseException
     */
    constructor(input: Partial<IDocument & T & any> = {}) {
        super(
            Object.entries(input) as [
                keyof (IDocument & T),
                (IDocument & T)[keyof (IDocument & T)],
            ][],
        );
        if (input.$permissions && !Array.isArray(input.$permissions)) {
            throw new DatabaseError("$permissions must be of type array");
        }

        for (const [key, value] of Object.entries(input)) {
            if (!Array.isArray(value)) {
                if (
                    value !== null &&
                    typeof value === "object" &&
                    ("$id" in value || "$collection" in value)
                ) {
                    this.set(
                        key as keyof (IDocument & T),
                        value instanceof Document
                            ? (value as (IDocument & T)[keyof (IDocument & T)])
                            : (new Document(
                                  value as Record<string, unknown>,
                              ) as unknown as (IDocument & T)[keyof (IDocument &
                                  T)]),
                    );
                } else {
                    this.set(
                        key as keyof (IDocument & T),
                        (value ?? null) as (IDocument & T)[keyof (IDocument &
                            T)],
                    );
                }
                continue;
            }

            // Handle array values
            if (
                value.some(
                    (item) =>
                        item !== null &&
                        item !== undefined &&
                        typeof item === "object" &&
                        (item["$id"] || item["$collection"]),
                )
            ) {
                const newValue = value.map((item) =>
                    item !== null &&
                    typeof item === "object" &&
                    (item["$id"] || item["$collection"])
                        ? item instanceof Document
                            ? item
                            : new Document(item as Record<string, unknown>)
                        : item,
                );
                this.set(
                    key as keyof (IDocument & T),
                    newValue as unknown as (IDocument & T)[keyof (IDocument &
                        T)],
                );
            } else {
                this.set(
                    key as keyof (IDocument & T),
                    value as unknown as (IDocument & T)[keyof (IDocument & T)],
                );
            }
        }
    }

    public getId(): string {
        return this.getAttribute("$id", "" as any) as string;
    }

    public getInternalId(): string {
        return String(this.getAttribute("$internalId", "" as any));
    }

    public getCollection(): string {
        return this.getAttribute("$collection", "" as any) as string;
    }

    public getPermissions(): string[] {
        return Array.from(
            new Set(this.getAttribute("$permissions", [] as any) as string[]),
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

    public getWrite(): string[] {
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

    public getCreatedAt(): string | null {
        return this.getAttribute("$createdAt", null) as any;
    }

    public getUpdatedAt(): string | null {
        return this.getAttribute("$updatedAt", null) as any;
    }

    public getAttributes(): Record<string, unknown> {
        const attributes: Record<string, unknown> = {};
        const internalKeys = Array.from(Database.INTERNAL_ATTRIBUTES).map(
            (attr) => (attr as any)["id"],
        );

        for (const [key, value] of this) {
            if (!internalKeys.includes(key as string)) {
                attributes[key as string] = value ?? null;
            }
        }

        return attributes;
    }

    public getAttribute<K extends keyof (IDocument & T)>(
        name: K,
        defaultValue: (IDocument & T)[K] | null = null,
    ): (IDocument & T)[K] | null {
        return this.has(name) && this.get(name) !== undefined
            ? (this.get(name) as (IDocument & T)[K])
            : defaultValue;
    }

    public setAttribute<K extends keyof (IDocument & T)>(
        key: K,
        value: (IDocument & T)[K] | unknown,
        type: string = Document.SET_TYPE_ASSIGN,
    ): this {
        switch (type) {
            case Document.SET_TYPE_ASSIGN:
                this.set(key, value as (IDocument & T)[keyof (IDocument & T)]);
                break;
            case Document.SET_TYPE_APPEND:
                const appendArray = this.get(key) || [];
                this.set(
                    key,
                    Array.isArray(appendArray)
                        ? ([...appendArray, value ?? null] as any)
                        : [value ?? null],
                );
                break;
            case Document.SET_TYPE_PREPEND:
                const prependArray = this.get(key) || [];
                this.set(
                    key,
                    Array.isArray(prependArray)
                        ? ([
                              value ?? null,
                              ...prependArray,
                          ] as unknown as (IDocument & T)[keyof (IDocument &
                              T)])
                        : ([value] as unknown as (IDocument &
                              T)[keyof (IDocument & T)]),
                );
                break;
        }
        return this;
    }

    public setAttributes(attributes: Partial<IDocument & T>): this {
        for (const [key, value] of Object.entries(attributes)) {
            this.setAttribute(
                key as keyof (IDocument & T),
                (value ?? null) as (IDocument & T)[keyof (IDocument & T)],
            );
        }
        return this;
    }

    public removeAttribute(key: keyof (IDocument & T)): this {
        this.delete(key);
        return this;
    }

    public find<V extends any>(
        key: keyof (IDocument & T),
        find: unknown | any,
        subject: keyof (IDocument & T) = "" as keyof (IDocument & T),
    ): V {
        const subjectData = this.get(subject) || this;
        if (Array.isArray(subjectData)) {
            return subjectData.find((value) => value[key] === find) || false;
        }
        return this.has(key) && this.get(key) === find
            ? (subjectData as any)
            : false;
    }

    public findAndReplace(
        key: keyof (IDocument & T),
        find: unknown,
        replace: unknown,
        subject: keyof (IDocument & T) = "" as keyof (IDocument & T),
    ): boolean {
        const subjectData = this.get(subject) || this;
        if (Array.isArray(subjectData)) {
            for (let i = 0; i < subjectData.length; i++) {
                if (subjectData[i][key] === find) {
                    subjectData[i] = replace;
                    return true;
                }
            }
            return false;
        }
        if (this.has(key) && this.get(key) === find) {
            this.set(key, replace as (IDocument & T)[keyof (IDocument & T)]);
            return true;
        }
        return false;
    }

    public findAndRemove(
        key: keyof (IDocument & T),
        find: unknown,
        subject: keyof (IDocument & T) = "" as keyof (IDocument & T),
    ): boolean {
        const subjectData = this.get(subject) || this;
        if (Array.isArray(subjectData)) {
            for (let i = 0; i < subjectData.length; i++) {
                if (subjectData[i][key] === find) {
                    subjectData.splice(i, 1);
                    return true;
                }
            }
            return false;
        }
        if (this.has(key) && this.get(key) === find) {
            this.delete(key);
            return true;
        }
        return false;
    }

    public isEmpty(): boolean {
        return this.size === 0;
    }

    public isSet(key: keyof (IDocument & T)): boolean {
        return this.has(key);
    }

    public getArrayCopy(
        allow: (keyof (IDocument & T))[] = [],
        disallow: (keyof (IDocument & T))[] = [],
    ): Record<string, unknown> {
        const output: Record<string, unknown> = {};
        for (const [key, value] of this) {
            if (allow.length && !allow.includes(key)) continue;
            if (disallow.includes(key)) continue;

            if (value instanceof Document) {
                output[key as string] = value.getArrayCopy(allow, disallow);
            } else if (Array.isArray(value)) {
                output[key as string] = value.map((item) =>
                    item instanceof Document
                        ? item.getArrayCopy(allow, disallow)
                        : item,
                );
            } else {
                output[key as string] = value;
            }
        }
        return output;
    }

    public toObj() {
        return this.getArrayCopy();
    }

    public clone(): this {
        const clonedDocument = new Document<T>();
        for (const [key, value] of this) {
            clonedDocument.set(
                key,
                value instanceof Document
                    ? (value.clone() as (IDocument & T)[keyof (IDocument & T)])
                    : (value as (IDocument & T)[keyof (IDocument & T)]),
            );
        }
        return clonedDocument as this;
    }
}
