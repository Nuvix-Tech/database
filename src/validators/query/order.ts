import { Base } from "./base.js";
import { Query } from "../../query";
import { Document } from "../../Document";

export class Order extends Base {
    protected schema: Record<string, any> = {};

    constructor(attributes: Document[] = []) {
        super();
        for (const attribute of attributes) {
            this.schema[
                attribute.getAttribute("key", attribute.getAttribute("$id"))
            ] = attribute.toObject();
        }
    }

    protected isValidAttribute(attribute: string): boolean {
        if (!this.schema[attribute]) {
            this.message = "Attribute not found in schema: " + attribute;
            return false;
        }
        return true;
    }

    public isValid(value: any): boolean {
        if (!(value instanceof Query)) {
            return false;
        }

        const method = value.getMethod();
        const attribute = value.getAttribute();

        if (
            method === Query.TYPE_ORDER_ASC ||
            method === Query.TYPE_ORDER_DESC
        ) {
            if (attribute === "") {
                return true;
            }
            return this.isValidAttribute(attribute);
        }

        return false;
    }

    public getMethodType(): string {
        return Base.METHOD_TYPE_ORDER;
    }
}
