import { Base } from "./base.js";
import { Query } from "../../query";
import { Constant as Database } from "../../constant";
import { Document } from "../../Document";

export class Select extends Base {
    protected schema: Record<string, any> = {};
    protected static INTERNAL_ATTRIBUTES = [
        "$id",
        "$internalId",
        "$createdAt",
        "$updatedAt",
        "$permissions",
        "$collection",
    ];

    constructor(attributes: Document[] = []) {
        super();
        for (const attribute of attributes) {
            this.schema[
                attribute.getAttribute("key", attribute.getAttribute("$id"))
            ] = attribute.toObject();
        }
    }

    public isValid(value: any): boolean {
        if (!(value instanceof Query)) {
            return false;
        }

        if (value.getMethod() !== Query.TYPE_SELECT) {
            return false;
        }

        const internalKeys = Object.values(Database.INTERNAL_ATTRIBUTES).map(
            (attr) => attr.$id,
        );

        for (let attribute of value.getValues()) {
            if (attribute.includes(".")) {
                if (this.schema[attribute]) {
                    continue;
                }
                attribute = attribute.split(".")[0];
            }

            if (internalKeys.includes(attribute)) {
                continue;
            }

            if (!this.schema[attribute] && attribute !== "*") {
                this.message = "Attribute not found in schema: " + attribute;
                return false;
            }
        }
        return true;
    }

    public getMethodType(): string {
        return Base.METHOD_TYPE_SELECT;
    }
}
