import { Doc } from "@core/doc.js";
import { Structure } from "./structure.js";
import { Database } from "@core/database.js";
import { Attribute } from "./schema.js";

export class PartialStructure extends Structure {
    /**
     * Is valid.
     *
     * Returns true if valid or false if not.
     *
     * @param document - The document to validate
     * @returns {boolean}
     */
    public override async $valid(document: unknown): Promise<boolean> {
        if (!(document instanceof Doc)) {
            this.message = "Value must be an instance of Doc";
            return false;
        }

        if (
            !this.collection.getId() ||
            this.collection.getCollection() !== Database.METADATA
        ) {
            this.message = "Collection not found";
            return false;
        }

        const structure: Record<string, unknown> = document.toObject();
        const attributes: Attribute[] = [
            ...this.systemAttributes,
            ...this.collection
                .get("attributes", [])
                .map((v: any) => (v instanceof Doc ? v.toObject() : v)),
        ];

        for (const attribute of attributes) {
            const name = attribute["$id"] ?? "";
            this.keys[name] = attribute;
        }

        if (!this.checkForUnknownAttributes(structure)) {
            return false;
        }

        if (!(await this.checkForInvalidAttributeValues(structure))) {
            return false;
        }

        return true;
    }
}
