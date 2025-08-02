import { Validator } from "./Validator";
import { Document } from "../Document";
import { Constant as Database } from "../constant";

export class Index extends Validator {
    protected message: string = "Invalid index";
    protected maxLength: number;
    protected attributes: Record<string, Document> = {};
    protected reservedKeys: string[];

    /**
     * Index constructor.
     *
     * @param attributes - Array of Document attributes
     * @param maxLength - Maximum length for the index
     * @param reservedKeys - Array of reserved keys
     * @throws DatabaseException
     */
    constructor(
        attributes: Document[],
        maxLength: number,
        reservedKeys: string[] = [],
    ) {
        super();
        this.maxLength = maxLength;
        this.reservedKeys = reservedKeys;

        for (const attribute of attributes as Document[]) {
            const key = attribute
                .getAttribute("key", attribute.getAttribute("$id"))
                .toLowerCase();
            this.attributes[key] = attribute;
        }

        // Assuming Database.INTERNAL_ATTRIBUTES is an array of Document attributes
        for (const attribute of Database.INTERNAL_ATTRIBUTES) {
            const key = attribute["$id"].toLowerCase();
            this.attributes[key] = new Document<typeof attribute>(attribute);
        }
    }

    /**
     * Returns validator description
     * @returns {string}
     */
    public getDescription(): string {
        return this.message;
    }

    /**
     * Check if attributes are not found in the index.
     * @param index - The index Document to validate
     * @returns {boolean}
     */
    public checkAttributesNotFound(index: Document): boolean {
        for (const attribute of index.getAttribute("attributes", [])) {
            if (!this.attributes.hasOwnProperty(attribute.toLowerCase())) {
                this.message = `Invalid index attribute "${attribute}" not found`;
                return false;
            }
        }
        return true;
    }

    /**
     * Check if index attributes are empty.
     * @param index - The index Document to validate
     * @returns {boolean}
     */
    public checkEmptyIndexAttributes(index: Document): boolean {
        if (index.getAttribute("attributes", []).length === 0) {
            this.message = "No attributes provided for index";
            return false;
        }
        return true;
    }

    /**
     * Check for duplicated attributes in the index.
     * @param index - The index Document to validate
     * @returns {boolean}
     */
    public checkDuplicatedAttributes(index: Document): boolean {
        const attributes = index.getAttribute("attributes", []);
        const stack: string[] = [];
        for (const attribute of attributes) {
            const value = attribute.toLowerCase();
            if (stack.includes(value)) {
                this.message = "Duplicate attributes provided";
                return false;
            }
            stack.push(value);
        }
        return true;
    }

    /**
     * Check if fulltext index has non-string attributes.
     * @param index - The index Document to validate
     * @returns {boolean}
     */
    public checkFulltextIndexNonString(index: Document): boolean {
        if (index.getAttribute("type") === Database.INDEX_FULLTEXT) {
            for (const attribute of index.getAttribute("attributes", [])) {
                const attrDoc =
                    this.attributes[attribute.toLowerCase()] || new Document();
                if (attrDoc.getAttribute("type", "") !== Database.VAR_STRING) {
                    this.message = `Attribute "${attrDoc.getAttribute("key", attrDoc.getAttribute("$id"))}" cannot be part of a FULLTEXT index, must be of type string`;
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Check if the index is an array index.
     * @param index - The index Document to validate
     * @returns {boolean}
     */
    public checkArrayIndex(index: Document): boolean {
        const attributes = index.getAttribute("attributes", []);
        const orders = index.getAttribute("orders", []);
        const lengths = index.getAttribute("lengths", []);

        const arrayAttributes: string[] = [];
        for (const attributePosition in attributes) {
            const attributeName = attributes[attributePosition];
            const attrDoc =
                this.attributes[attributeName.toLowerCase()] || new Document();

            if (attrDoc.getAttribute("array", false)) {
                if (index.getAttribute("type") !== Database.INDEX_KEY) {
                    this.message = `"${capitalize(index.getAttribute("type"))}" index is forbidden on array attributes`;
                    return false;
                }

                if (!lengths[attributePosition]) {
                    this.message = "Index length for array not specified";
                    return false;
                }

                arrayAttributes.push(attrDoc.getAttribute("key", ""));
                if (arrayAttributes.length > 1) {
                    this.message =
                        "An index may only contain one array attribute";
                    return false;
                }

                const direction = orders[attributePosition] || "";
                if (direction) {
                    this.message = `Invalid index order "${direction}" on array attribute "${attrDoc.getAttribute("key", "")}"`;
                    return false;
                }
            } else if (
                attrDoc.getAttribute("type") !== Database.VAR_STRING &&
                lengths[attributePosition]
            ) {
                this.message = `Cannot set a length on "${attrDoc.getAttribute("type")}" attributes`;
                return false;
            }
        }
        return true;
    }

    /**
     * Check the index length.
     * @param index - The index Document to validate
     * @returns {boolean}
     */
    public checkIndexLength(index: Document): boolean {
        if (index.getAttribute("type") === Database.INDEX_FULLTEXT) {
            return true;
        }

        let total = 0;
        const lengths = index.getAttribute("lengths", []);

        for (const attributePosition in index.getAttribute("attributes", [])) {
            const attributeName =
                index.getAttribute("attributes")[attributePosition];
            const attrDoc = this.attributes[attributeName.toLowerCase()]!;

            let indexLength: number;
            switch (attrDoc.getAttribute("type")) {
                case Database.VAR_STRING:
                    const attributeSize = attrDoc.getAttribute("size", 0);
                    indexLength = lengths[attributePosition] || attributeSize;
                    break;
                case Database.VAR_FLOAT:
                    indexLength = 2; // 8 bytes / 4 mb4
                    break;
                default:
                    indexLength = 1; // 4 bytes / 4 mb4
                    break;
            }

            if (attrDoc.getAttribute("array", false)) {
                indexLength = Database.ARRAY_INDEX_LENGTH;
            }

            if (
                attrDoc.getAttribute("size", 0) !== 0 &&
                indexLength > attrDoc.getAttribute("size", 0)
            ) {
                this.message = `Index length ${indexLength} is larger than the size for ${attributeName}: ${attrDoc.getAttribute("size", 0)}`;
                return false;
            }

            total += indexLength;
        }

        if (total > this.maxLength && this.maxLength > 0) {
            this.message = `Index length is longer than the maximum: ${this.maxLength}`;
            return false;
        }

        return true;
    }

    /**
     * Check for reserved names in the index.
     * @param index - The index Document to validate
     * @returns {boolean}
     */
    public checkReservedNames(index: Document): boolean {
        const key = index.getAttribute("key", index.getAttribute("$id"));

        for (const reserved of this.reservedKeys) {
            if (key.toLowerCase() === reserved.toLowerCase()) {
                this.message = "Index key name is reserved";
                return false;
            }
        }

        return true;
    }

    /**
     * Is valid.
     *
     * Returns true if the index is valid.
     * @param value - The index Document to validate
     * @returns {boolean}
     * @throws DatabaseException
     */
    public isValid(value: Document): boolean {
        if (!this.checkAttributesNotFound(value)) return false;
        if (!this.checkEmptyIndexAttributes(value)) return false;
        if (!this.checkDuplicatedAttributes(value)) return false;
        if (!this.checkFulltextIndexNonString(value)) return false;
        if (!this.checkArrayIndex(value)) return false;
        if (!this.checkIndexLength(value)) return false;
        if (!this.checkReservedNames(value)) return false;

        return true;
    }

    /**
     * Is array
     *
     * Function will return true if object is array.
     *
     * @returns {boolean}
     */
    public isArray(): boolean {
        return false;
    }

    /**
     * Get Type
     *
     * Returns validator type.
     *
     * @returns {string}
     */
    public getType(): string {
        return "object"; // Assuming you want to return a string representation of the type
    }
}

// Helper function to capitalize the first letter of a string
function capitalize(str: string): string {
    return str.charAt(0).toUpperCase() + str.slice(1);
}
