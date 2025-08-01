import { QueryException } from "@errors/index.js";

export enum QueryType {
    Equal = "equal",
    NotEqual = "notEqual",
    LessThan = "lessThan",
    LessThanEqual = "lessThanEqual",
    GreaterThan = "greaterThan",
    GreaterThanEqual = "greaterThanEqual",
    Contains = "contains",
    Search = "search",
    IsNull = "isNull",
    IsNotNull = "isNotNull",
    Between = "between",
    StartsWith = "startsWith",
    EndsWith = "endsWith",
    Select = "select",
    OrderDesc = "orderDesc",
    OrderAsc = "orderAsc",
    Limit = "limit",
    Offset = "offset",
    CursorAfter = "cursorAfter",
    CursorBefore = "cursorBefore",
    And = "and",
    Or = "or",
}

export class Query {

    public static readonly TYPES = Object.values(QueryType);

    protected static readonly LOGICAL_TYPES = [
        QueryType.And,
        QueryType.Or,
    ]

    protected method: QueryType;
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
    constructor(method: QueryType, attribute: string = "", values: any[] = []) {
        if (
            attribute === "" &&
            [QueryType.OrderAsc, QueryType.OrderDesc].includes(method as QueryType)
        ) {
            attribute = "$sequence";
        }

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
    public getMethod(): QueryType {
        return this.method as QueryType;
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
    public setMethod(method: QueryType): this {
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
    public static isMethod(value: QueryType): boolean {
        return Query.TYPES.includes(value);
    }

    /**
     * Parse query
     *
     * @param query
     * @returns {Query}
     * @throws {QueryException}
     */
    public static parse(query: string): Query {
        try {
            const parsedQuery = JSON.parse(query);
            return Query.parseQuery(parsedQuery);
        } catch (e: any) {
            throw new QueryException("Invalid query: " + e.message);
        }
    }

    /**
     * Parse query
     *
     * @param query
     * @returns {Query}
     * @throws {QueryException}
     */
    public static parseQuery(query: any): Query {
        const method: QueryType = query.method || "";
        const attribute = query.attribute || "";
        const values = query.values || [];

        if (typeof method !== "string") {
            throw new QueryException(
                "Invalid query method. Must be a string.",
            );
        }

        if (!Query.isMethod(method)) {
            throw new QueryException("Invalid query method: " + method);
        }

        if (typeof attribute !== "string") {
            throw new QueryException(
                "Invalid query attribute. Must be a string.",
            );
        }

        if (!Array.isArray(values)) {
            throw new QueryException(
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
     * @throws {QueryException}
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
            [QueryType.CursorAfter, QueryType.CursorBefore].includes(
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
     * @throws {QueryException}
     */
    public toString(): string {
        try {
            return JSON.stringify(this.toArray());
        } catch (e: any) {
            throw new QueryException("Invalid JSON: " + e.message);
        }
    }

    // Helper methods for creating specific query types
    public static equal(
        attribute: string,
        values: (string | number | boolean | any)[],
    ): Query {
        return new Query(QueryType.Equal, attribute, values);
    }

    public static notEqual(
        attribute: string,
        value: string | number | boolean | any,
    ): Query {
        return new Query(QueryType.NotEqual, attribute, [value]);
    }

    public static lessThan(attribute: string, value: number | string): Query {
        return new Query(QueryType.LessThan, attribute, [value]);
    }

    public static lessThanEqual(
        attribute: string,
        value: number | string,
    ): Query {
        return new Query(QueryType.LessThanEqual, attribute, [value]);
    }

    public static greaterThan(
        attribute: string,
        value: number | string,
    ): Query {
        return new Query(QueryType.GreaterThan, attribute, [value]);
    }

    public static greaterThanEqual(
        attribute: string,
        value: number | string,
    ): Query {
        return new Query(QueryType.GreaterThanEqual, attribute, [value]);
    }

    public static contains(
        attribute: string,
        values: (string | number | boolean | any)[],
    ): Query {
        return new Query(QueryType.Contains, attribute, values);
    }

    public static between(
        attribute: string,
        start: string | number | boolean | any,
        end: string | number | boolean | any,
    ): Query {
        return new Query(QueryType.Between, attribute, [start, end]);
    }

    public static search(attribute: string, value: string): Query {
        return new Query(QueryType.Search, attribute, [value]);
    }

    public static select(attributes: string[]): Query {
        return new Query(QueryType.Select, "", attributes);
    }

    public static orderDesc(attribute: string): Query {
        return new Query(QueryType.OrderDesc, attribute);
    }

    public static orderAsc(attribute: string): Query {
        return new Query(QueryType.OrderAsc, attribute);
    }

    public static limit(value: number): Query {
        return new Query(QueryType.Limit, "", [value]);
    }

    public static offset(value: number): Query {
        return new Query(QueryType.Offset, "", [value]);
    }

    public static cursorAfter(value: string): Query {
        return new Query(QueryType.CursorAfter, "", [value]);
    }

    public static cursorBefore(value: string): Query {
        return new Query(QueryType.CursorBefore, "", [value]);
    }

    public static isNull(attribute: string): Query {
        return new Query(QueryType.IsNull, attribute);
    }

    public static isNotNull(attribute: string): Query {
        return new Query(QueryType.IsNotNull, attribute);
    }

    public static startsWith(attribute: string, value: string): Query {
        return new Query(QueryType.StartsWith, attribute, [value]);
    }

    public static endsWith(attribute: string, value: string): Query {
        return new Query(QueryType.EndsWith, attribute, [value]);
    }

    public static or(queries: Query[]): Query {
        return new Query(QueryType.Or, "", queries);
    }

    public static and(queries: Query[]): Query {
        return new Query(QueryType.And, "", queries);
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
        cursor: string | number | null;
        cursorDirection: 'AFTER' | 'BEFORE' | null;
    } {
        const filters: Query[] = [];
        const selections: Query[] = [];
        let limit: number | null = null;
        let offset: number | null = null;
        const orderAttributes: string[] = [];
        const orderTypes: string[] = [];
        let cursor: string | number | null = null;
        let cursorDirection: 'AFTER' | 'BEFORE' | null = null;

        for (const query of queries) {
            const method = query.getMethod();
            const attribute = query.getAttribute();
            const values = query.getValues();

            switch (method) {
                case QueryType.OrderAsc:
                case QueryType.OrderDesc:
                    if (attribute) {
                        orderAttributes.push(attribute);
                    }
                    orderTypes.push(
                        method === QueryType.OrderAsc ? "ASC" : "DESC",
                    );
                    break;
                case QueryType.Limit:
                    if (limit === null) {
                        limit = values[0] ?? limit;
                    }
                    break;
                case QueryType.Offset:
                    if (offset === null) {
                        offset = values[0] ?? offset;
                    }
                    break;
                case QueryType.CursorAfter:
                case QueryType.CursorBefore:
                    if (cursor === null) {
                        cursor = values[0] ?? cursor;
                        cursorDirection =
                            method === QueryType.CursorAfter
                                ? "AFTER"
                                : "BEFORE";
                    }
                    break;
                case QueryType.Select:
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
