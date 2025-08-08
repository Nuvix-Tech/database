import { AttributeEnum, RelationEnum, RelationSideEnum } from "@core/enums.js";
import { Format, Validator } from "./interface.js";
import { Attribute, Collection, RelationOptions } from "./schema.js";
import { Doc } from "@core/doc.js";
import { DatabaseException } from "@errors/base.js";
import { Database } from "@core/database.js";
import { Text } from "./text.js";
import { Integer } from "./Integer.js";
import { Range } from "./range.js";
import { NumericType } from "./numeric.js";
import { FloatValidator } from "./float-validator.js";
import { Boolean } from "./boolean.js";
import { Datetime } from "./datetime.js";
import { Json } from "./json.js";
import { UUID } from "./uuid.js";

/**
 * Validates the structure and data types of a document against a collection's schema.
 * It combines system attributes with custom collection attributes.
 */
export class Structure implements Validator {
    protected systemAttributes: Attribute[] = [
        {
            $id: "$id",
            key: "$id",
            type: AttributeEnum.String,
            size: 255,
        },
        {
            $id: "$sequence",
            key: "$sequence",
            type: AttributeEnum.Integer,
            size: 8,
        },
        {
            $id: "$collection",
            key: "$collection",
            type: AttributeEnum.String,
            size: 255,
            required: true,
        },
        {
            $id: "$tenant",
            key: "$tenant",
            type: AttributeEnum.Integer,
            default: null,
            size: 8,
        },
        {
            $id: "$permissions",
            key: "$permissions",
            type: AttributeEnum.String,
            size: 255,
            array: true,
        },
        {
            $id: "$createdAt",
            key: "$createdAt",
            type: AttributeEnum.Timestamptz,
        },
        {
            $id: "$updatedAt",
            key: "$updatedAt",
            type: AttributeEnum.Timestamptz,
        },
    ];

    protected static formats: Record<
        string,
        Format
    > = {};

    protected message: string = "Invalid document structure.";
    protected keys: Record<string, Attribute> = {};
    private onCreate: boolean = false;

    /**
     * Structure constructor.
     *
     * @param collection - The collection Doc whose attributes define the schema for validation.
     * @param minAllowedDate - The minimum date allowed for datetime attributes.
     * @param maxAllowedDate - The maximum date allowed for datetime attributes.
     */
    constructor(
        protected readonly collection: Doc<Collection>,
        private readonly minAllowedDate: Date = new Date("0000-01-01"),
        private readonly maxAllowedDate: Date = new Date("9999-12-31"),
    ) { }

    /**
     * Get the registry of custom validation formats.
     *
     * @returns {Record<string, Format>} A record of format names to their definitions.
     */
    public static getFormats(): Record<
        string,
        Format
    > {
        return this.formats;
    }

    /**
     * Add a new custom validation format to the registry.
     *
     * @param name - The name of the format (e.g., "email", "url").
     * @param callback - A callback function that accepts parameters and returns a Validator instance.
     * @param type - The primitive data type this format validator applies to (e.g., "string", "integer").
     */
    public static addFormat(
        name: string,
        format: Format,
    ): void {
        this.formats[name] = format;
    }

    /**
     * Check if a specific format validator has been added.
     *
     * @param name - The name of the format.
     * @param type - The primitive data type of the format.
     * @returns {boolean} True if the format exists for the given type, false otherwise.
     */
    public static hasFormat(name: string, type: AttributeEnum): boolean {
        return (
            (this.formats[name] && this.formats[name].type === type) || false
        );
    }

    /**
     * Get a registered format validator callback and its type.
     *
     * @param name - The name of the format.
     * @param type - The primitive data type of the format.
     * @returns {Format} The format definition.
     * @throws {DatabaseException} If the format is unknown or not available for the specified type.
     */
    public static getFormat(
        name: string,
        type: AttributeEnum,
    ): Format {
        if (this.formats[name]) {
            if (this.formats[name].type !== type) {
                throw new DatabaseException(
                    `Format "${name}" not available for attribute type "${type}".`,
                );
            }
            return this.formats[name];
        }
        throw new DatabaseException(`Unknown format validator "${name}".`);
    }

    /**
     * Remove a custom validation format from the registry.
     *
     * @param name - The name of the format to remove.
     */
    public static removeFormat(name: string): void {
        delete this.formats[name];
    }

    /**
     * Get the validator's description (error message).
     *
     * @returns {string} The error message, prefixed with "Invalid document structure: ".
     */
    public get $description(): string {
        return "Invalid document structure: " + this.message;
    }

    /**
     * Validates a document's structure and values against the collection's schema.
     * This is the main public validation method.
     *
     * @param document - The document `unknown` to validate.
     * @returns {boolean} True if the document is valid, false otherwise.
     */
    public async $valid(document: unknown): Promise<boolean> {
        this.message = "Invalid document structure.";

        if (!(document instanceof Doc)) {
            this.message = "Value must be an instance of Doc.";
            return false;
        }

        if (!document.getCollection()) {
            this.message = "Missing collection ID on document.";
            return false;
        }

        if (
            !this.collection.getId() ||
            this.collection.getCollection() !== Database.METADATA
        ) {
            this.message = "Collection not found or is not a valid metadata collection.";
            return false;
        }

        const documentStructure: Record<string, unknown> = document.toObject();

        const allAttributes: Attribute[] = [
            ...this.systemAttributes,
            ...(this.collection
                .get("attributes", []) as unknown[])
                .map((v: unknown) =>
                    v instanceof Doc ? v.toObject() as Attribute : v as Attribute
                ),
        ];

        if (!this.checkForAllRequiredValues(documentStructure, allAttributes)) {
            return false;
        }

        if (!this.checkForUnknownAttributes(documentStructure)) {
            return false;
        }

        if (!(await this.checkForInvalidAttributeValues(documentStructure))) {
            return false;
        }

        return true;
    }

    /**
     * Populates `this.keys` and checks if all required attributes are present in the document.
     *
     * @param documentStructure - The document data as a plain object.
     * @param attributes - The combined list of all expected attribute schemas (system + collection).
     * @returns {boolean} True if all required attributes are present, false otherwise.
     */
    protected checkForAllRequiredValues(
        documentStructure: Record<string, unknown>,
        attributes: Attribute[],
    ): boolean {
        this.keys = {};

        for (const attribute of attributes) {
            const id = attribute.$id;
            const required = attribute.required ?? false;

            this.keys[id] = attribute;

            if (required && !(id in documentStructure)) {
                this.message = `Missing required attribute "${id}".`;
                return false;
            }
        }

        return true;
    }

    /**
     * Checks if the document contains any attributes not defined in the schema.
     *
     * @param documentStructure - The document data as a plain object.
     * @returns {boolean} True if no unknown attributes are found, false otherwise.
     */
    protected checkForUnknownAttributes(
        documentStructure: Record<string, unknown>,
    ): boolean {
        for (const key in documentStructure) {
            if (!this.keys.hasOwnProperty(key)) {
                this.message = `Unknown attribute: "${key}".`;
                return false;
            }
        }
        return true;
    }

    /**
     * Validates the values of attributes against their defined types and rules.
     *
     * @param documentStructure - The document data as a plain object.
     * @returns {boolean} True if all attribute values are valid, false otherwise.
     */
    protected async checkForInvalidAttributeValues(
        documentStructure: Record<string, unknown>,
    ): Promise<boolean> {
        for (const key in documentStructure) {
            if (key === '$permissions') continue;
            const value = documentStructure[key];
            const attribute: Attribute | undefined = this.keys[key];

            if (!attribute) {
                this.message = `Internal error: Attribute "${key}" schema not found.`;
                return false;
            }

            const type = attribute.type;
            const isArray = attribute.array ?? false;
            const required = attribute.required ?? false;
            const size = attribute.size ?? 0;

            if (
                type === AttributeEnum.Virtual
                || (!required && (value === null || value === undefined))
            ) {
                continue;
            }

            if (type === AttributeEnum.Relationship) {
                const valid = this.checkForInvalidRelationshipValues(attribute, value);
                if (!valid) {
                    return false;
                }
                continue;
            }

            const attributeValidators: Validator[] = [];
            switch (type) {
                case AttributeEnum.String:
                    attributeValidators.push(new Text(size, 0));
                    break;

                case AttributeEnum.Integer: {
                    attributeValidators.push(new Integer());
                    const max = size && size >= 8 ? Database.BIG_INT_MAX : Database.INT_MAX;
                    attributeValidators.push(new Range(-max, max, NumericType.INTEGER));
                    break;
                }

                case AttributeEnum.Float: {
                    attributeValidators.push(new FloatValidator());
                    const floatMin = -Database.DOUBLE_MAX;
                    attributeValidators.push(
                        new Range(
                            floatMin,
                            Database.DOUBLE_MAX,
                            NumericType.FLOAT,
                        ),
                    );
                    break;
                }

                case AttributeEnum.Boolean:
                    attributeValidators.push(new Boolean());
                    break;

                case AttributeEnum.Timestamptz:
                    attributeValidators.push(
                        new Datetime(
                            this.minAllowedDate,
                            this.maxAllowedDate,
                        ),
                    );
                    break;
                case AttributeEnum.Json:
                    attributeValidators.push(
                        new Json(),
                    );
                    break;
                case AttributeEnum.Uuid:
                    attributeValidators.push(
                        new UUID(),
                    );
                    break;
                default:
                    this.message = `Unknown or unsupported attribute type "${type}" for attribute "${key}".`;
                    return false;
            }

            if (isArray) {
                if (!required && ((Array.isArray(value) && value.length === 0) || value === null)) {
                    continue;
                }

                if (!Array.isArray(value)) {
                    this.message = `Attribute "${key}" must be an array.`;
                    return false;
                }

                for (const [index, child] of value.entries()) {
                    if (!required && child === null) {
                        continue;
                    }

                    for (const validator of attributeValidators) {
                        let valid = validator.$valid(child);
                        if (valid instanceof Promise) {
                            valid = await valid.catch(() => false);
                        }
                        if (!valid) {
                            this.message = `Attribute "${key}[${index}]" has an invalid value. ${validator.$description}`;
                            return false;
                        }
                    }
                }
            } else {
                for (const validator of attributeValidators) {
                    let valid = validator.$valid(value);
                    if (valid instanceof Promise) {
                        valid = await valid.catch(() => false);
                    }
                    if (!valid) {
                        this.message = `Attribute "${key}" has an invalid value. ${validator.$description}`;
                        return false;
                    }
                }
            }
        }

        return true;
    }

    protected checkForInvalidRelationshipValues(attr: Attribute, value: any): boolean {
        const options = attr.options as RelationOptions;
        const type = options.relationType;
        const side = options.side;

        switch (type) {
            case RelationEnum.OneToOne:
                if (!value && typeof value !== "string") {
                    this.message = `Attribute "${attr.key}" must be a string or null for OneToOne relation.`;
                    return false;
                }
                return true;
            case RelationEnum.ManyToMany:
                return this.checkForInvalidManyValues(value, attr.key);
            case RelationEnum.OneToMany:
                if (side === RelationSideEnum.Parent) {
                    return this.checkForInvalidManyValues(value, attr.key);
                }
                if (side === RelationSideEnum.Child) {
                    if (!value && typeof value !== "string") {
                        this.message = `Attribute "${attr.key}" must be a string or null for OneToMany relation on child side.`;
                        return false;
                    }
                    return true;
                }
                this.message = `Unknown relation side "${side}" for attribute "${attr.key}".`;
                return false;
            case RelationEnum.ManyToOne:
                if (side === RelationSideEnum.Parent) {
                    if (!value && typeof value !== "string") {
                        this.message = `Attribute "${attr.key}" must be a string or null for ManyToOne relation on parent side.`;
                        return false;
                    }
                    return true;
                }
                if (side === RelationSideEnum.Child) {
                    return this.checkForInvalidManyValues(value, attr.key);
                }
                this.message = `Unknown relation side "${side}" for attribute "${attr.key}".`;
                return false;
            default:
                this.message = `Unknown relation type "${type}" for attribute "${attr.key}".`;
                return false;
        }
    }

    private checkForInvalidManyValues(value: any, attributeKey: string): boolean {
        if (!value || typeof value !== 'object') {
            this.message = `Attribute "${attributeKey}" must be an object for to-many relation operations.`;
            return false;
        }

        if ('set' in value || 'connect' in value || 'disconnect' in value) {
            // Validate set operation
            if ('set' in value) {
                if (!Array.isArray(value.set)) {
                    this.message = `Attribute "${attributeKey}.set" must be an array of document IDs.`;
                    return false;
                }
                for (const id of value.set) {
                    if (typeof id !== 'string' || id.trim() === '') {
                        this.message = `Attribute "${attributeKey}.set" contains invalid document ID.`;
                        return false;
                    }
                }
            }

            // Validate connect operation
            if ('connect' in value) {
                if (!Array.isArray(value.connect)) {
                    this.message = `Attribute "${attributeKey}.connect" must be an array of document IDs.`;
                    return false;
                }
                for (const id of value.connect) {
                    if (typeof id !== 'string' || id.trim() === '') {
                        this.message = `Attribute "${attributeKey}.connect" contains invalid document ID.`;
                        return false;
                    }
                }
            }

            // Validate disconnect operation
            if ('disconnect' in value) {
                if (!Array.isArray(value.disconnect)) {
                    this.message = `Attribute "${attributeKey}.disconnect" must be an array of document IDs.`;
                    return false;
                }
                for (const id of value.disconnect) {
                    if (typeof id !== 'string' || id.trim() === '') {
                        this.message = `Attribute "${attributeKey}.disconnect" contains invalid document ID.`;
                        return false;
                    }
                }
            }

            return true;
        }

        if ('set' in value && ('connect' in value || 'disconnect' in value)) {
            this.message = `Attribute "${attributeKey}" must not contain both "set" and "connect"/"disconnect" operations.`;
            return false;
        }

        if (this.onCreate && !('set' in value)) {
            this.message = `Attribute "${attributeKey}" must contain "set" operation on creation.`;
            return false;
        }

        this.message = `Attribute "${attributeKey}" must contain at least one of: set, connect, or disconnect operations.`;
        return false;
    }

    setOnCreate(onCreate: boolean): void {
        this.onCreate = onCreate;
    }
}
