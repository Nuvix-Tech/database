import { Queries } from "@validators/queries.js";
import { Select } from "../query/select.js";
import { Doc } from "@core/doc.js";
import { AttributeEnum } from "@core/enums.js";
import { Attribute } from "@validators/schema.js";
import { Database } from "@core/database.js";

export class Document extends Queries {

    /**
     * Document constructor.
     *
     * @param attributes - Array of attributes
     */
    constructor(attributes: Doc<Attribute>[] = []) {
        const workingAttributes = Array.isArray(attributes)
            ? [...attributes]
            : [];

        workingAttributes.push(
            new Doc({
                $id: "$id",
                key: "$id",
                type: AttributeEnum.String,
                array: false,
                size: Database.LENGTH_KEY
            }),
            new Doc({
                $id: "$createdAt",
                key: "$createdAt",
                type: AttributeEnum.Datetime,
                array: false,
            }),
            new Doc({
                $id: "$updatedAt",
                key: "$updatedAt",
                type: AttributeEnum.Datetime,
                array: false,
            }),
        );

        super([new Select(workingAttributes)]);
    }
}
