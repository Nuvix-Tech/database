import { IndexedQueries } from "../IndexedQueries.js";
import { Constant as Database } from "../../constant";
import { Document as DatabaseDocument } from "../../Document";
import { Limit } from "../query/limit.js";
import { Offset } from "../query/offset.js";
import { Cursor } from "../query/cursor.js";
import { Filter } from "../query/filter.js";
import { Order } from "../query/order.js";
import { Select } from "../query/select.js";

export class Documents extends IndexedQueries {
    /**
     * Documents constructor
     *
     * @param attributes - Array of attributes
     * @param indexes - Array of indexes
     * @param maxValuesCount - Maximum number of values allowed
     * @param minAllowedDate - Minimum allowed date
     * @param maxAllowedDate - Maximum allowed date
     */
    constructor(
        attributes: DatabaseDocument[] = [],
        indexes: any[], // Adjust the type based on your index structure
        maxValuesCount: number = 100,
        minAllowedDate: Date = new Date("0000-01-01"),
        maxAllowedDate: Date = new Date("9999-12-31"),
    ) {
        // Add default attributes
        attributes.push(
            new DatabaseDocument({
                $id: "$id",
                key: "$id",
                type: Database.VAR_STRING,
                array: false,
            }),
        );
        attributes.push(
            new DatabaseDocument({
                $id: "$internalId",
                key: "$internalId",
                type: Database.VAR_STRING,
                array: false,
            }),
        );
        attributes.push(
            new DatabaseDocument({
                $id: "$createdAt",
                key: "$createdAt",
                type: Database.VAR_DATETIME,
                array: false,
            }),
        );
        attributes.push(
            new DatabaseDocument({
                $id: "$updatedAt",
                key: "$updatedAt",
                type: Database.VAR_DATETIME,
                array: false,
            }),
        );

        const validators = [
            new Limit(),
            new Offset(),
            new Cursor(),
            new Filter(
                attributes,
                maxValuesCount,
                minAllowedDate,
                maxAllowedDate,
            ),
            new Order(attributes),
            new Select(attributes),
        ];

        super(attributes, indexes, validators);
    }
}
