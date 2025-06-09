import { DatabaseError as DatabaseException } from "../errors/base";

export class Query {
    // Filter methods
    public static readonly TYPE_EQUAL = "equal";
    public static readonly TYPE_NOT_EQUAL = "notEqual";
    public static readonly TYPE_LESSER = "lessThan";
    public static readonly TYPE_LESSER_EQUAL = "lessThanEqual";
    public static readonly TYPE_GREATER = "greaterThan";
    public static readonly TYPE_GREATER_EQUAL = "greaterThanEqual";
    public static readonly TYPE_CONTAINS = "contains";
    public static readonly TYPE_SEARCH = "search";
    public static readonly TYPE_IS_NULL = "isNull";
    public static readonly TYPE_IS_NOT_NULL = "isNotNull";
    public static readonly TYPE_BETWEEN = "between";
    public static readonly TYPE_STARTS_WITH = "startsWith";
    public static readonly TYPE_ENDS_WITH = "endsWith";

    public static readonly TYPE_SELECT = "select";

    // Order methods
    public static readonly TYPE_ORDER_DESC = "orderDesc";
    public static readonly TYPE_ORDER_ASC = "orderAsc";

    // Pagination methods
    public static readonly TYPE_LIMIT = "limit";
    public static readonly TYPE_OFFSET = "offset";
    public static readonly TYPE_CURSOR_AFTER = "cursorAfter";
    public static readonly TYPE_CURSOR_BEFORE = "cursorBefore";

    // Logical methods
    public static readonly TYPE_AND = "and";
    public static readonly TYPE_OR = "or";

    public static readonly TYPES = [
        Query.TYPE_EQUAL,
        Query.TYPE_NOT_EQUAL,
        Query.TYPE_LESSER,
        Query.TYPE_LESSER_EQUAL,
        Query.TYPE_GREATER,
        Query.TYPE_GREATER_EQUAL,
        Query.TYPE_CONTAINS,
        Query.TYPE_SEARCH,
        Query.TYPE_IS_NULL,
        Query.TYPE_IS_NOT_NULL,
        Query.TYPE_BETWEEN,
        Query.TYPE_STARTS_WITH,
        Query.TYPE_ENDS_WITH,
        Query.TYPE_SELECT,
        Query.TYPE_ORDER_DESC,
        Query.TYPE_ORDER_ASC,
        Query.TYPE_LIMIT,
        Query.TYPE_OFFSET,
        Query.TYPE_CURSOR_AFTER,
        Query.TYPE_CURSOR_BEFORE,
        Query.TYPE_AND,
        Query.TYPE_OR,
    ];

    protected static readonly LOGICAL_TYPES = [Query.TYPE_AND, Query.TYPE_OR];

    protected method: string;
    protected attribute: string;
    protected _onArray: boolean;
    protected values: any[];

    /**
     * Construct a new query object
     *
     * @param method
     * @param attribute
     * @param values
     */
    constructor(method: string, attribute: string = "", values: any[] = []) {
        this.method = method;
        this.attribute = attribute;
        this.values = values;
        this._onArray = false;
    }

    public clone(): Query {
        const clonedQuery = new Query(this.method, this.attribute, [
            ...this.values,
        ]);
        return clonedQuery;
    }

    /**
     * @returns {string}
     */
    public getMethod(): string {
        return this.method;
    }

    /**
     * @returns {string}
     */
    public getAttribute(): string {
        return this.attribute;
    }

    /**
     * @returns {any[]}
     */
    public getValues(): any[] {
        return this.values;
    }

    /**
     * @param defaultValue
     * @returns {any}
     */
    public getValue(defaultValue: any = null): any {
        return this.values[0] ?? defaultValue;
    }

    /**
     * Sets method
     *
     * @param method
     * @returns {this}
     */
    public setMethod(method: string): this {
        this.method = method;
        return this;
    }

    /**
     * Sets attribute
     *
     * @param attribute
     * @returns {this}
     */
    public setAttribute(attribute: string): this {
        this.attribute = attribute;
        return this;
    }

    /**
     * Sets values
     *
     * @param values
     * @returns {this}
     */
    public setValues(values: any[]): this {
        this.values = values;
        return this;
    }

    /**
     * Sets value
     * @param value
     * @returns {this}
     */
    public setValue(value: any): this {
        this.values = [value];
        return this;
    }

    /**
     * Check if method is supported
     *
     * @param value
     * @ returns {boolean}
     */
    public static isMethod(value: string): boolean {
        return Query.TYPES.includes(value);
    }

    /**
     * Parse query
     *
     * @param query
     * @returns {Query}
     * @throws {DatabaseException}
     */
    public static parse(query: string): Query {
        try {
            const parsedQuery = JSON.parse(query);
            return Query.parseQuery(parsedQuery);
        } catch (e: any) {
            throw new DatabaseException("Invalid query: " + e.message);
        }
    }

    /**
     * Parse query
     *
     * @param query
     * @returns {Query}
     * @throws {DatabaseException}
     */
    public static parseQuery(query: any): Query {
        const method = query.method || "";
        const attribute = query.attribute || "";
        const values = query.values || [];

        if (typeof method !== "string") {
            throw new DatabaseException(
                "Invalid query method. Must be a string.",
            );
        }

        if (!Query.isMethod(method)) {
            throw new DatabaseException("Invalid query method: " + method);
        }

        if (typeof attribute !== "string") {
            throw new DatabaseException(
                "Invalid query attribute. Must be a string.",
            );
        }

        if (!Array.isArray(values)) {
            throw new DatabaseException(
                "Invalid query values. Must be an array.",
            );
        }

        if (Query.LOGICAL_TYPES.includes(method)) {
            for (let i = 0; i < values.length; i++) {
                values[i] = Query.parseQuery(values[i]);
            }
        }

        return new Query(method, attribute, values);
    }

    /**
     * Parse an array of queries
     *
     * @param queries
     * @returns {Query[]}
     * @throws {DatabaseException}
     */
    public static parseQueries(queries: string[]): Query[] {
        return queries.map((query) => Query.parse(query));
    }

    /**
     * Find a cursor query in an array of queries
     *
     * @param queries
     * @returns {Query | undefined}
     */
    public static findCursor(queries: Query[]): Query | undefined {
        return queries.find((query) =>
            [Query.TYPE_CURSOR_AFTER, Query.TYPE_CURSOR_BEFORE].includes(
                query.getMethod(),
            ),
        );
    }

    /**
     * Convert query to array representation
     *
     * @returns {object}
     */
    public toArray(): object {
        const array: any = { method: this.method };

        if (this.attribute) {
            array.attribute = this.attribute;
        }

        if (Query.LOGICAL_TYPES.includes(this.method)) {
            array.values = this.values.map((value) => value.toArray());
        } else {
            array.values = this.values;
        }

        return array;
    }

    /**
     * Convert query to string representation
     *
     * @returns {string}
     * @throws {DatabaseException}
     */
    public toString(): string {
        try {
            return JSON.stringify(this.toArray());
        } catch (e: any) {
            throw new DatabaseException("Invalid JSON: " + e.message);
        }
    }

    // Helper methods for creating specific query types
    public static equal(
        attribute: string,
        values: (string | number | boolean | any)[],
    ): Query {
        return new Query(Query.TYPE_EQUAL, attribute, values);
    }

    public static notEqual(
        attribute: string,
        value: string | number | boolean | any,
    ): Query {
        return new Query(Query.TYPE_NOT_EQUAL, attribute, [value]);
    }

    public static lessThan(attribute: string, value: number | string): Query {
        return new Query(Query.TYPE_LESSER, attribute, [value]);
    }

    public static lessThanEqual(
        attribute: string,
        value: number | string,
    ): Query {
        return new Query(Query.TYPE_LESSER_EQUAL, attribute, [value]);
    }

    public static greaterThan(
        attribute: string,
        value: number | string,
    ): Query {
        return new Query(Query.TYPE_GREATER, attribute, [value]);
    }

    public static greaterThanEqual(
        attribute: string,
        value: number | string,
    ): Query {
        return new Query(Query.TYPE_GREATER_EQUAL, attribute, [value]);
    }

    public static contains(
        attribute: string,
        values: (string | number | boolean | any)[],
    ): Query {
        return new Query(Query.TYPE_CONTAINS, attribute, values);
    }

    public static between(
        attribute: string,
        start: string | number | boolean | any,
        end: string | number | boolean | any,
    ): Query {
        return new Query(Query.TYPE_BETWEEN, attribute, [start, end]);
    }

    public static search(attribute: string, value: string): Query {
        return new Query(Query.TYPE_SEARCH, attribute, [value]);
    }

    public static select(attributes: string[]): Query {
        return new Query(Query.TYPE_SELECT, "", attributes);
    }

    public static orderDesc(attribute: string): Query {
        return new Query(Query.TYPE_ORDER_DESC, attribute);
    }

    public static orderAsc(attribute: string): Query {
        return new Query(Query.TYPE_ORDER_ASC, attribute);
    }

    public static limit(value: number): Query {
        return new Query(Query.TYPE_LIMIT, "", [value]);
    }

    public static offset(value: number): Query {
        return new Query(Query.TYPE_OFFSET, "", [value]);
    }

    public static cursorAfter(value: string): Query {
        return new Query(Query.TYPE_CURSOR_AFTER, "", [value]);
    }

    public static cursorBefore(value: string): Query {
        return new Query(Query.TYPE_CURSOR_BEFORE, "", [value]);
    }

    public static isNull(attribute: string): Query {
        return new Query(Query.TYPE_IS_NULL, attribute);
    }

    public static isNotNull(attribute: string): Query {
        return new Query(Query.TYPE_IS_NOT_NULL, attribute);
    }

    public static startsWith(attribute: string, value: string): Query {
        return new Query(Query.TYPE_STARTS_WITH, attribute, [value]);
    }

    public static endsWith(attribute: string, value: string): Query {
        return new Query(Query.TYPE_ENDS_WITH, attribute, [value]);
    }

    public static or(queries: Query[]): Query {
        return new Query(Query.TYPE_OR, "", queries);
    }

    public static and(queries: Query[]): Query {
        return new Query(Query.TYPE_AND, "", queries);
    }

    public static getByType(queries: Query[], types: string[]): Query[] {
        return queries
            .filter((query) => types.includes(query.getMethod()))
            .map((query) => query.clone());
    }

    public static groupByType(queries: Query[]): {
        filters: Query[];
        selections: Query[];
        limit: number | null;
        offset: number | null;
        orderAttributes: string[];
        orderTypes: string[];
        cursor: any | null;
        cursorDirection: string | null;
    } {
        const filters: Query[] = [];
        const selections: Query[] = [];
        let limit: number | null = null;
        let offset: number | null = null;
        const orderAttributes: string[] = [];
        const orderTypes: string[] = [];
        let cursor: any | null = null;
        let cursorDirection: string | null = null;

        for (const query of queries) {
            const method = query.getMethod();
            const attribute = query.getAttribute();
            const values = query.getValues();

            switch (method) {
                case Query.TYPE_ORDER_ASC:
                case Query.TYPE_ORDER_DESC:
                    if (attribute) {
                        orderAttributes.push(attribute);
                    }
                    orderTypes.push(
                        method === Query.TYPE_ORDER_ASC ? "ASC" : "DESC",
                    );
                    break;
                case Query.TYPE_LIMIT:
                    if (limit === null) {
                        limit = values[0] ?? limit;
                    }
                    break;
                case Query.TYPE_OFFSET:
                    if (offset === null) {
                        offset = values[0] ?? offset;
                    }
                    break;
                case Query.TYPE_CURSOR_AFTER:
                case Query.TYPE_CURSOR_BEFORE:
                    if (cursor === null) {
                        cursor = values[0] ?? cursor;
                        cursorDirection =
                            method === Query.TYPE_CURSOR_AFTER
                                ? "AFTER"
                                : "BEFORE";
                    }
                    break;
                case Query.TYPE_SELECT:
                    selections.push(query.clone());
                    break;
                default:
                    filters.push(query.clone());
                    break;
            }
        }

        return {
            filters,
            selections,
            limit,
            offset,
            orderAttributes,
            orderTypes,
            cursor,
            cursorDirection,
        };
    }

    public isNested(): boolean {
        return Query.LOGICAL_TYPES.includes(this.method);
    }

    public onArray(): boolean {
        return this._onArray;
    }

    public setOnArray(bool: boolean): void {
        this._onArray = bool;
    }
}
