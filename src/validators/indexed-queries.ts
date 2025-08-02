import { Doc } from "@core/doc.js";
import { Queries } from "./queries.js";
import { Attribute, Index } from "./schema.js";
import { Base } from "./query/base.js";
import { IndexEnum } from "@core/enums.js";
import { Query, QueryType } from "@core/query.js";

export class IndexedQueries extends Queries {
    protected attributes: Doc<Attribute>[] = [];
    protected indexes: Doc<Index>[] = [];

    /**
     * IndexedQueries constructor
     *
     * @param attributes - An array of `Doc<Attribute>` objects representing the collection's attributes.
     * @param indexes - An array of `Doc<Index>` objects representing the collection's defined indexes.
     * @param validators - An optional array of `Base` specific query validators to pass to the super `Queries` class.
     */
    constructor(
        attributes: Doc<Attribute>[] = [],
        indexes: Doc<Index>[] = [],
        validators: Base[] = [],
    ) {
        super(validators);

        this.attributes = attributes;
        this.indexes = [
            new Doc<Index>({
                $id: "$id",
                type: IndexEnum.Key,
                attributes: ["$id"],
            }),
            new Doc<Index>({
                $id: "$createdAt",
                type: IndexEnum.Key,
                attributes: ["$createdAt"],
            }),
            new Doc<Index>({
                $id: "$updatedAt",
                type: IndexEnum.Key,
                attributes: ["$updatedAt"],
            }),
        ];

        for (const index of indexes) {
            this.indexes.push(index);
        }
    }

    /**
     * Overrides the base `$valid` method to add index-specific validation.
     * This method first delegates to the superclass's validation and then
     * performs additional checks, such as ensuring full-text indexes for search queries.
     *
     * @param value - The value to validate, expected to be an array of Query objects or parsable query structures.
     * @returns {boolean} True if the queries are valid, false otherwise.
     */
    public override $valid(value: unknown): boolean {
        if (!super.$valid(value)) {
            return false;
        }

        const queries: Query[] = value as Query[];

        const groupedQueries = Query.groupByType(queries);
        const filters = groupedQueries.filters;

        for (const filter of filters) {
            if (filter.getMethod() === QueryType.Search) {
                let matchedIndex = false;

                for (const index of this.indexes) {
                    if (
                        index.get("type") === IndexEnum.FullText &&
                        index.get("attributes")?.includes(filter.getAttribute())
                    ) {
                        matchedIndex = true;
                        break;
                    }
                }

                if (!matchedIndex) {
                    this.message = `Searching by attribute "${filter.getAttribute()}" requires a fulltext index.`;
                    return false;
                }
            }
        }

        return true;
    }
}
