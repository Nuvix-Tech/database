import { Queries } from "./index.js";
import { Constant as Database } from "../../constant";
import { Document as DatabaseDocument } from "../../Document";
import { Select } from "../query/select.js";

export class Document extends Queries {
    protected attributes: DatabaseDocument[];

    /**
     * Document constructor.
     *
     * @param attributes - Array of attributes
     */
    constructor(attributes: DatabaseDocument[] = []) {
        // Initialize validators first
        const workingAttributes = Array.isArray(attributes)
            ? [...attributes]
            : [];

        // Add default attributes
        workingAttributes.push(
            new DatabaseDocument({
                $id: "$id",
                key: "$id",
                type: Database.VAR_STRING,
                array: false,
            }),
        );

        workingAttributes.push(
            new DatabaseDocument({
                $id: "$createdAt",
                key: "$createdAt",
                type: Database.VAR_DATETIME,
                array: false,
            }),
        );

        workingAttributes.push(
            new DatabaseDocument({
                $id: "$updatedAt",
                key: "$updatedAt",
                type: Database.VAR_DATETIME,
                array: false,
            }),
        );

        // Call super with validators
        super([new Select(workingAttributes)]);

        // Initialize class properties after super()
        this.attributes = workingAttributes;
    }

    /**
     * Get document attributes
     */
    public getAttributes(): DatabaseDocument[] {
        return this.attributes;
    }
}
