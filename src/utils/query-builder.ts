import { Doc } from "@core/doc.js";
import { Query } from "@core/query.js";

export class QueryBuilder {
    private queries: Query[] = [];

    /**
     * Create a new QueryBuilder instance
     */
    static from(queries: Query[]): QueryBuilder {
        const builder = new QueryBuilder();
        builder.queries = queries;
        return builder;
    }

    /**
     * Add an equality condition
     * @param attribute Attribute name
     * @param values Values to match
     */
    equal(attribute: string, ...values: (string | number | boolean)[]): this {
        this.queries.push(Query.equal(attribute, values));
        return this;
    }

    /**
     * Add a not equal condition
     * @param attribute Attribute name
     * @param value Value to exclude
     */
    notEqual(attribute: string, value: string | number | boolean): this {
        this.queries.push(Query.notEqual(attribute, value));
        return this;
    }

    /**
     * Add a less than condition
     * @param attribute Attribute name
     * @param value Upper bound value
     */
    lessThan(attribute: string, value: number | string): this {
        this.queries.push(Query.lessThan(attribute, value));
        return this;
    }

    /**
     * Add a less than or equal condition
     * @param attribute Attribute name
     * @param value Upper bound value
     */
    lessThanEqual(attribute: string, value: number | string): this {
        this.queries.push(Query.lessThanEqual(attribute, value));
        return this;
    }

    /**
     * Add a greater than condition
     * @param attribute Attribute name
     * @param value Lower bound value
     */
    greaterThan(attribute: string, value: number | string): this {
        this.queries.push(Query.greaterThan(attribute, value));
        return this;
    }

    /**
     * Add a greater than or equal condition
     * @param attribute Attribute name
     * @param value Lower bound value
     */
    greaterThanEqual(attribute: string, value: number | string): this {
        this.queries.push(Query.greaterThanEqual(attribute, value));
        return this;
    }

    /**
     * Add a contains condition (array contains values)
     * @param attribute Attribute name
     * @param values Values to check for
     */
    contains(attribute: string, ...values: any[]): this {
        this.queries.push(Query.contains(attribute, values));
        return this;
    }

    /**
     * Add a between condition
     * @param attribute Attribute name
     * @param start Start value
     * @param end End value
     */
    between(attribute: string, start: any, end: any): this {
        this.queries.push(Query.between(attribute, start, end));
        return this;
    }

    /**
     * Add a full-text search condition
     * @param attribute Attribute name
     * @param value Search term
     */
    search(attribute: string, value: string): this {
        this.queries.push(Query.search(attribute, value));
        return this;
    }

    /**
     * Select specific attributes to return
     * @param attributes Attributes to select
     */
    select(...attributes: string[]): this {
        this.queries.push(Query.select(attributes));
        return this;
    }

    /**
     * Add descending order
     * @param attribute Attribute to sort by (default: '$sequence')
     */
    orderDesc(attribute: string = ""): this {
        this.queries.push(Query.orderDesc(attribute));
        return this;
    }

    /**
     * Add ascending order
     * @param attribute Attribute to sort by (default: '$sequence')
     */
    orderAsc(attribute: string = ""): this {
        this.queries.push(Query.orderAsc(attribute));
        return this;
    }

    /**
     * Set result limit
     * @param value Maximum number of results
     */
    limit(value: number): this {
        this.queries.push(Query.limit(value));
        return this;
    }

    /**
     * Set result offset
     * @param value Number of results to skip
     */
    offset(value: number): this {
        this.queries.push(Query.offset(value));
        return this;
    }

    /**
     * Set cursor position (after)
     * @param value Cursor document or ID
     */
    cursorAfter(value: Doc | string): this {
        const cursorValue = value instanceof Doc ? value.getId() : value;
        this.queries.push(Query.cursorAfter(cursorValue));
        return this;
    }

    /**
     * Set cursor position (before)
     * @param value Cursor document or ID
     */
    cursorBefore(value: Doc | string): this {
        const cursorValue = value instanceof Doc ? value.getId() : value;
        this.queries.push(Query.cursorBefore(cursorValue));
        return this;
    }

    /**
     * Check if attribute is null
     * @param attribute Attribute name
     */
    isNull(attribute: string): this {
        this.queries.push(Query.isNull(attribute));
        return this;
    }

    /**
     * Check if attribute is not null
     * @param attribute Attribute name
     */
    isNotNull(attribute: string): this {
        this.queries.push(Query.isNotNull(attribute));
        return this;
    }

    /**
     * Check if attribute starts with value
     * @param attribute Attribute name
     * @param value String prefix
     */
    startsWith(attribute: string, value: string): this {
        this.queries.push(Query.startsWith(attribute, value));
        return this;
    }

    /**
     * Check if attribute ends with value
     * @param attribute Attribute name
     * @param value String suffix
     */
    endsWith(attribute: string, value: string): this {
        this.queries.push(Query.endsWith(attribute, value));
        return this;
    }

    /**
     * Add an OR group of conditions
     * @param builderFn Builder function for nested conditions
     */
    or(builderFn: (builder: QueryBuilder) => void): this {
        const nestedBuilder = new QueryBuilder();
        builderFn(nestedBuilder);
        this.queries.push(Query.or(nestedBuilder.build()));
        return this;
    }

    /**
     * Add an AND group of conditions
     * @param builderFn Builder function for nested conditions
     */
    and(builderFn: (builder: QueryBuilder) => void): this {
        const nestedBuilder = new QueryBuilder();
        builderFn(nestedBuilder);
        this.queries.push(Query.and(nestedBuilder.build()));
        return this;
    }

    /**
     * Mark that the query should operate on arrays
     * @param enable Whether to enable array mode
     */
    onArray(enable: boolean = true): this {
        if (this.queries.length > 0) {
            this.queries[this.queries.length - 1]?.setOnArray(enable);
        }
        return this;
    }

    /**
     * Build the final query array
     */
    build(): Query[] {
        return [...this.queries];
    }

    /**
     * Clear the current builder state
     */
    clear(): this {
        this.queries = [];
        return this;
    }

    // Utility methods for common patterns
    /**
     * Add pagination parameters
     * @param limit Number of results per page
     * @param offset Page offset
     */
    paginate(limit: number, offset: number): this {
        return this.limit(limit).offset(offset);
    }

    /**
     * Add ID-based cursor pagination
     * @param cursorId Cursor document ID
     * @param direction Pagination direction
     */
    cursorPaginate(cursorId: string, direction: "after" | "before" = "after"): this {
        return direction === "after"
            ? this.cursorAfter(cursorId)
            : this.cursorBefore(cursorId);
    }

    /**
     * Add text search across multiple fields
     * @param fields Fields to search
     * @param term Search term
     */
    multiSearch(fields: string[], term: string): this {
        const builder = new QueryBuilder();
        fields.forEach(field => {
            builder.search(field, term);
        });
        this.queries.push(Query.or(builder.build()));
        return this;
    }
}
