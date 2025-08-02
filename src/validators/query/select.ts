import { Doc } from "@core/doc.js";
import { Base, MethodType } from "./base.js";
import { Query, QueryType } from "@core/query.js";
import { Database } from "@core/database.js";
import { Attribute } from "@validators/schema.js";

export class Select extends Base {
    protected schema: Record<string, any>;
    protected static readonly INTERNAL_ATTRIBUTES = [
        "$id",
        "$internalId",
        "$createdAt",
        "$updatedAt",
        "$permissions",
        "$collection",
    ];

    constructor(attributes: Doc<Attribute>[] = []) {
        super();
        this.schema = {};
        for (const attribute of attributes) {
            const key = attribute.get("key", attribute.get("$id"));
            this.schema[key] = attribute.toObject();
        }
    }

    public $valid(value: unknown): boolean {
        if (!(value instanceof Query)) return false;
        if (value.getMethod() !== QueryType.Select) return false;

        const internalKeys = Database.INTERNAL_ATTRIBUTES.map(attr => attr.$id);

        for (const rawAttribute of value.getValues()) {
            let attribute = rawAttribute as string;
            if (attribute.includes(".")) {
                const [baseAttr] = attribute.split(".");
                if (this.schema[attribute]) continue;
                attribute = baseAttr!;
            }

            if (internalKeys.includes(attribute)) continue;
            if (!this.schema[attribute] && attribute !== "*") {
                this.message = `Attribute not found in schema: ${attribute}`;
                return false;
            }
        }
        return true;
    }

    public getMethodType(): MethodType {
        return MethodType.Select;
    }
}
