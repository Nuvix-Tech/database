import { DatabaseError } from "../errors/base";
import { Constant as Database } from "./constant";

export interface IDocument {
    $permissions: string[];
    $createdAt: string | null;
    $updatedAt: string | null;
    $id: string;
    $internalId: string;
    $collection: string;
}

// Type aliases for improved readability
export type DocumentKey<T> = keyof (T & IDocument) | string;
export type DocumentValue<T> = (T & IDocument)[keyof (T & IDocument)] | unknown;
export type SetType =
    | typeof Document.SET_TYPE_ASSIGN
    | typeof Document.SET_TYPE_PREPEND
    | typeof Document.SET_TYPE_APPEND;

/**
 * Document class with improved typing for IDE autocompletion
 */
export class Document<
    T extends Partial<Record<string, unknown> & IDocument> = any,
> extends Map<DocumentKey<T>, DocumentValue<T>> {
    public static readonly SET_TYPE_ASSIGN = "assign";
    public static readonly SET_TYPE_PREPEND = "prepend";
    public static readonly SET_TYPE_APPEND = "append";

    /**
     * Construct a new Document object
     *
     * @param input - Initial data for the document
     * @throws DatabaseError
     */
    constructor(input: Partial<T & IDocument> = {}) {
        super(Object.entries(input) as [DocumentKey<T>, DocumentValue<T>][]);
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
                        key as DocumentKey<T>,
                        value instanceof Document
                            ? (value as DocumentValue<T>)
                            : (new Document(
                                  value as Record<string, unknown>,
                              ) as unknown as DocumentValue<T>),
                    );
                } else {
                    this.set(
                        key as DocumentKey<T>,
                        (value ?? null) as DocumentValue<T>,
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
                    key as DocumentKey<T>,
                    newValue as unknown as DocumentValue<T>,
                );
            } else {
                this.set(
                    key as DocumentKey<T>,
                    value as unknown as DocumentValue<T>,
                );
            }
        }
    }

    /**
     * Get the document ID
     * @returns The document ID string
     */
    public getId(): string {
        return this.getAttribute("$id", "" as any) as string;
    }

    /**
     * Get the document internal ID
     * @returns The internal ID string
     */
    public getInternalId(): string {
        return String(this.getAttribute("$internalId", "" as any));
    }

    /**
     * Get the collection name
     * @returns The collection name string
     */
    public getCollection(): string {
        return this.getAttribute("$collection", "" as any) as string;
    }

    /**
     * Get document permissions
     * @returns Array of permission strings
     */
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

    /**
     * Get document creation timestamp
     * @returns Creation timestamp or null
     */
    public getCreatedAt(): string | null {
        return this.getAttribute("$createdAt", null) as any;
    }

    /**
     * Get document last update timestamp
     * @returns Last update timestamp or null
     */
    public getUpdatedAt(): string | null {
        return this.getAttribute("$updatedAt", null) as any;
    }

    /**
     * Get all non-internal attributes as a plain object
     * @returns Record of attribute key-value pairs
     */
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

    /**
     * Get an attribute with type safety
     * - If attribute exists, returns its value with proper type
     * - If attribute doesn't exist, returns the default value with its type
     * - Supports chaining via method returns
     *
     * @param name - Attribute name
     * @returns Attribute value with proper type
     */
    public getAttribute<K extends keyof (T & IDocument)>(
        name: K,
    ): K extends keyof (T & IDocument) ? (T & IDocument)[K] : null;

    /**
     * Get an attribute with type safety and custom default value
     * - If attribute exists, returns its value with proper type
     * - If attribute doesn't exist, returns the default value with its type
     * - Supports chaining via method returns
     *
     * @param name - Attribute name
     * @param defaultValue - Default value if attribute doesn't exist
     * @returns Attribute value or default value with combined types
     */
    public getAttribute<K extends keyof (T & IDocument), D>(
        name: K,
        defaultValue: D,
    ): K extends keyof (T & IDocument) ? (T & IDocument)[K] : D;

    /**
     * Get an attribute with type safety for unknown attributes
     * - For attributes not defined in T, returns unknown type
     * - Supports chaining via method returns
     *
     * @param name - Attribute name (string key not in T)
     * @returns Unknown value or null
     */
    public getAttribute<T = unknown>(name: string): T | null;

    /**
     * Get an attribute with type safety for unknown attributes with default
     * - For attributes not defined in T, returns default value type
     * - Supports chaining via method returns
     *
     * @param name - Attribute name (string key not in T)
     * @param defaultValue - Default value if attribute doesn't exist
     * @returns Unknown value or default value
     */
    public getAttribute<D>(name: string, defaultValue: D): D;

    public getAttribute<K extends keyof (T & IDocument) | string, D>(
        name: K,
        defaultValue: D | null | undefined = null,
    ): (T & IDocument)[keyof (T & IDocument)] | D | unknown | null {
        return this.has(name) && this.get(name) !== undefined
            ? this.get(name)
            : defaultValue;
    }

    /**
     * Set an attribute with assignment
     * @param key - Attribute key
     * @param value - New value
     * @returns This document instance for chaining
     * @template K - Key type
     * @template V - Value type
     */
    public setAttribute<
        K extends keyof (T & IDocument),
        V extends (T & IDocument)[K],
    >(key: K, value: V): this;

    /**
     * Set an attribute with assignment (unknown attribute)
     * @param key - Attribute key (not defined in T)
     * @param value - New value
     * @returns This document instance for chaining with updated type
     * @template K - String key not in T
     * @template V - Any value type
     */
    public setAttribute<K extends string, V>(key: K, value: V): this;

    /**
     * Set an attribute with specified operation type
     * @param key - Attribute key
     * @param value - New value
     * @param type - Operation type (assign, append, or prepend)
     * @returns This document instance for chaining
     */
    public setAttribute<
        K extends keyof (T & IDocument),
        V extends (T & IDocument)[K],
    >(key: K, value: V, type: SetType): this;

    /**
     * Set an attribute with specified operation type (unknown attribute)
     * @param key - Attribute key (not defined in T)
     * @param value - New value
     * @param type - Operation type (assign, append, or prepend)
     * @returns This document instance for chaining with updated type
     */
    public setAttribute<K extends string, V>(
        key: K,
        value: V,
        type: SetType,
    ): this;

    public setAttribute<K extends keyof (T & IDocument) | string, V>(
        key: K,
        value: V,
        type: SetType = Document.SET_TYPE_ASSIGN,
    ): this {
        switch (type) {
            case Document.SET_TYPE_ASSIGN:
                this.set(key, value as DocumentValue<T>);
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
                          ] as unknown as DocumentValue<T>)
                        : ([value] as unknown as DocumentValue<T>),
                );
                break;
        }
        return this;
    }

    /**
     * Set multiple attributes at once
     * @param attributes - Object with attributes to set
     * @returns This document instance for chaining with updated type
     * @template U - Type of attributes being added
     */
    public setAttributes<U extends Partial<Record<string, unknown>>>(
        attributes: U,
    ): this {
        for (const [key, value] of Object.entries(attributes)) {
            this.setAttribute(
                key as DocumentKey<T> as any,
                (value ?? null) as DocumentValue<T>,
            );
        }
        return this;
    }

    /**
     * Remove an attribute
     * @param key - Attribute key to remove
     * @returns This document instance for chaining with updated type
     * @template K - Key to remove
     */
    public removeAttribute<K extends keyof T>(key: K): this {
        this.delete(key as DocumentKey<T>);
        return this;
    }

    /**
     * Find an item by key and value
     * @param key - Key to match
     * @param find - Value to match
     * @returns Found value or false if not found
     */
    public find<V = any>(key: DocumentKey<T>, find: unknown | null): V | false;

    /**
     * Find an item by key and value within a specific subject
     * @param key - Key to match
     * @param find - Value to match
     * @param subject - Subject key to search within
     * @returns Found value or false if not found
     */
    public find<V = any>(
        key: DocumentKey<T>,
        find: unknown | null,
        subject: DocumentKey<T>,
    ): V | false;

    public find<V = any>(
        key: DocumentKey<T>,
        find: unknown | null,
        subject: DocumentKey<T> = "" as DocumentKey<T>,
    ): V | false {
        const subjectData = this.get(subject) || this;

        if (Array.isArray(subjectData)) {
            for (const item of subjectData) {
                if (item instanceof Document) {
                    const result = item.find(key, find, subject);
                    if (result) return result as any;
                } else if (item[key] === find) {
                    return item as V;
                }
            }
            return false;
        }

        if (this.has(key)) {
            const value = this.get(key);
            if (value === find) {
                return subjectData as V;
            }

            if (value instanceof Document) {
                return value.find(key, find, subject);
            }

            if (
                (typeof value === "string" || typeof value === "number") &&
                value === find
            ) {
                return subjectData as V;
            }

            if (value instanceof Map) {
                for (const [mapKey, mapValue] of value) {
                    if (mapKey === key && mapValue === find) {
                        return value as V;
                    }
                }
            }
        }

        return false;
    }

    /**
     * Find and replace a value
     * @param key - Key to match
     * @param find - Value to match
     * @param replace - Value to replace with
     * @returns True if replaced, false otherwise
     */
    public findAndReplace(
        key: DocumentKey<T>,
        find: unknown,
        replace: unknown,
    ): boolean;

    /**
     * Find and replace a value within a specific subject
     * @param key - Key to match
     * @param find - Value to match
     * @param replace - Value to replace with
     * @param subject - Subject key to search within
     * @returns True if replaced, false otherwise
     */
    public findAndReplace(
        key: DocumentKey<T>,
        find: unknown,
        replace: unknown,
        subject: DocumentKey<T>,
    ): boolean;

    public findAndReplace(
        key: DocumentKey<T>,
        find: unknown,
        replace: unknown,
        subject: DocumentKey<T> = "" as DocumentKey<T>,
    ): boolean {
        const subjectData = this.get(subject) || this;
        if (Array.isArray(subjectData)) {
            for (let i = 0; i < subjectData.length; i++) {
                if (subjectData[i] instanceof Document) {
                    if (subjectData[i].find(key, find)) {
                        subjectData[i] = replace;
                        return true;
                    }
                } else if (subjectData[i][key] === find) {
                    subjectData[i] = replace;
                    return true;
                }
            }
            return false;
        }
        if (this.has(key) && this.get(key) === find) {
            this.set(key, replace as DocumentValue<T>);
            return true;
        }
        return false;
    }

    /**
     * Find and remove a value
     * @param key - Key to match
     * @param find - Value to match
     * @returns True if removed, false otherwise
     */
    public findAndRemove(key: DocumentKey<T>, find: unknown): boolean;

    /**
     * Find and remove a value within a specific subject
     * @param key - Key to match
     * @param find - Value to match
     * @param subject - Subject key to search within
     * @returns True if removed, false otherwise
     */
    public findAndRemove(
        key: DocumentKey<T>,
        find: unknown,
        subject: DocumentKey<T>,
    ): boolean;

    public findAndRemove(
        key: DocumentKey<T>,
        find: unknown,
        subject: DocumentKey<T> = "" as DocumentKey<T>,
    ): boolean {
        const subjectData = this.get(subject) || this;
        if (Array.isArray(subjectData)) {
            for (let i = 0; i < subjectData.length; i++) {
                if (subjectData[i] instanceof Document) {
                    if (subjectData[i].find(key, find)) {
                        subjectData.splice(i, 1);
                        return true;
                    }
                } else if (subjectData[i][key] === find) {
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

    /**
     * Check if document has no attributes
     * @returns True if empty
     */
    public isEmpty(): boolean {
        return this.size === 0;
    }

    /**
     * Check if an attribute is set
     * @param key - Attribute key (defined in T or unknown)
     * @returns True if attribute is set
     */
    public isSet(key: keyof (T & IDocument) | string): boolean {
        return this.has(key);
    }

    /**
     * Convert document to a plain object with all attributes
     * @returns Plain object representation
     */
    public toObject(): Record<string, unknown>;

    /**
     * Convert document to a plain object with specific allowed attributes
     * @param allow - Keys to include (empty means all)
     * @param disallow - Keys to exclude
     * @returns Plain object representation
     */
    public toObject(
        allow: DocumentKey<T>[],
        disallow?: DocumentKey<T>[],
    ): Record<string, unknown>;

    public toObject(
        allow: DocumentKey<T>[] = [],
        disallow: DocumentKey<T>[] = [],
    ): Record<string, unknown> {
        const output: Record<string, unknown> = {};
        for (const [key, value] of this) {
            if (allow.length && !allow.includes(key)) continue;
            if (disallow.includes(key)) continue;

            if (value instanceof Document) {
                output[key as string] = value.toObject(allow, disallow);
            } else if (Array.isArray(value)) {
                output[key as string] = value.map((item) =>
                    item instanceof Document
                        ? item.toObject(allow, disallow)
                        : item,
                );
            } else {
                output[key as string] = value;
            }
        }
        return output;
    }

    /**
     * @deprecated Use toObject instead
     */
    public toObj(): Record<string, unknown> {
        return this.toObject();
    }

    /**
     * Convert document to JSON representation
     * @returns Plain object for JSON serialization
     */
    public toJSON(): Record<string, unknown> {
        return this.toObject();
    }

    /**
     * Create a deep clone of this document
     * @returns A new document with the same attributes
     */
    public clone(): this {
        const clonedDocument = new Document<T>();
        for (const [key, value] of this) {
            clonedDocument.set(
                key,
                value instanceof Document
                    ? (value.clone() as DocumentValue<T>)
                    : (value as DocumentValue<T>),
            );
        }
        return clonedDocument as this;
    }
}
