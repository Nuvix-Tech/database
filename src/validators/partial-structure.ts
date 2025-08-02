import { Structure } from "./Structure";
import { Document } from "../Document";
import { Constant as Database } from "../constant";

export class PartialStructure extends Structure {
    /**
     * Is valid.
     *
     * Returns true if valid or false if not.
     *
     * @param document - The document to validate
     * @returns {boolean}
     */
    public override isValid(document: any): boolean {
        if (!(document instanceof Document)) {
            this.message = "Value must be an instance of Document";
            return false;
        }

        if (
            !this.collection.getId() ||
            this.collection.getCollection() !== Database.METADATA
        ) {
            this.message = "Collection not found";
            return false;
        }

        const structure = document.toObject();
        const attributes = [
            ...this.attributes,
            ...this.collection
                .getAttribute("attributes", [])
                .map((v: any) => (v instanceof Document ? v.toObject() : v)),
        ];

        for (const attribute of attributes) {
            const name = attribute["$id"] ?? "";
            this.keys[name] = attribute;
        }

        if (!this.checkForUnknownAttributes(structure)) {
            return false;
        }

        if (!this.checkForInvalidAttributeValues(structure)) {
            return false;
        }

        return true;
    }
}
