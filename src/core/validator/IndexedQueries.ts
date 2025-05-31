import { Queries } from "./Queries";
import { Constant as Database } from "../constant";
import { Document } from "../Document";
import { Query } from "../query";
import { Base } from "./Query/Base";

export class IndexedQueries extends Queries {
    protected attributes: Document[] = [];
    protected indexes: Document[] = [];

    /**
     * IndexedQueries constructor
     *
     * @param attributes - Array of attributes
     * @param indexes - Array of indexes
     * @param validators - Array of validators
     */
    constructor(
        attributes: Document[] = [],
        indexes: Document[] = [],
        validators: Base[] = [],
    ) {
        super(validators);
        this.attributes = attributes;

        // Add default indexes
        this.indexes.push(
            new Document({
                type: Database.INDEX_UNIQUE,
                attributes: ["$id"],
            }),
        );

        this.indexes.push(
            new Document({
                type: Database.INDEX_KEY,
                attributes: ["$createdAt"],
            }),
        );

        this.indexes.push(
            new Document({
                type: Database.INDEX_KEY,
                attributes: ["$updatedAt"],
            }),
        );

        // Add provided indexes
        for (const index of indexes) {
            this.indexes.push(index);
        }
    }

    /**
     * Is valid.
     *
     * Returns true if valid or false if not.
     *
     * @param value - The value to validate
     * @returns {boolean}
     */
    public override isValid(value: any): boolean {
        if (!super.isValid(value)) {
            return false;
        }

        const queries: Query[] = [];
        for (let query of value) {
            if (!(query instanceof Query)) {
                try {
                    query = Query.parse(query);
                } catch (error) {
                    this.message = "Invalid query: " + (error as Error).message;
                    return false;
                }
            }

            if (query.isNested()) {
                if (!this.isValid(query.getValues())) {
                    return false;
                }
            }

            queries.push(query);
        }

        const grouped = Query.groupByType(queries);
        const filters = grouped.filters;

        for (const filter of filters) {
            if (filter.getMethod() === Query.TYPE_SEARCH) {
                let matched = false;

                for (const index of this.indexes) {
                    if (
                        index.getAttribute("type") ===
                            Database.INDEX_FULLTEXT &&
                        index
                            .getAttribute("attributes")
                            .includes(filter.getAttribute())
                    ) {
                        matched = true;
                        break;
                    }
                }
                if (!matched) {
                    this.message = `Searching by attribute "${filter.getAttribute()}" requires a fulltext index.`;
                    return false;
                }
            }
        }

        return true;
    }
}
