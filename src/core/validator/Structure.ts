import { Validator } from "./Validator";
import { Document } from "../Document";
import { Constant as Database } from "../constant";
import {
    BooleanValidator,
    DatetimeValidator,
    FloatValidator,
    IntegerValidator,
    RangeValidator,
    TextValidator,
} from ".";
import { DatabaseError as DatabaseException } from "../../errors/base";

export class Structure extends Validator {
    protected attributes: Record<string, any>[] = [
        {
            $id: "$id",
            type: Database.VAR_STRING,
            size: 255,
            required: false,
            signed: true,
            array: false,
            filters: [],
        },
        {
            $id: "$internalId",
            type: Database.VAR_INTEGER,
            size: 255,
            required: false,
            signed: true,
            array: false,
            filters: [],
        },
        {
            $id: "$collection",
            type: Database.VAR_STRING,
            size: 255,
            required: true,
            signed: true,
            array: false,
            filters: [],
        },
        {
            $id: "$tenant",
            type: Database.VAR_STRING,
            size: 36,
            required: false,
            default: null,
            signed: true,
            array: false,
            filters: [],
        },
        {
            $id: "$permissions",
            type: Database.VAR_STRING,
            size: 67000, // medium text
            required: false,
            signed: true,
            array: true,
            filters: [],
        },
        {
            $id: "$createdAt",
            type: Database.VAR_DATETIME,
            size: 0,
            required: false,
            signed: false,
            array: false,
            filters: [],
        },
        {
            $id: "$updatedAt",
            type: Database.VAR_DATETIME,
            size: 0,
            required: false,
            signed: false,
            array: false,
            filters: [],
        },
    ];

    protected static formats: Record<
        string,
        { callback: (params: any) => Validator; type: string }
    > = {};

    protected message: string = "General Error";

    /**
     * Structure constructor.
     *
     * @param collection - The collection Document
     * @param minAllowedDate - Minimum allowed date
     * @param maxAllowedDate - Maximum allowed date
     */
    constructor(
        protected readonly collection: Document,
        private readonly minAllowedDate: Date = new Date("0000-01-01"),
        private readonly maxAllowedDate: Date = new Date("9999-12-31"),
    ) {
        super();
    }

    /**
     * Get Formats.
     *
     * @returns {Record<string, { callback: (params: any) => Validator, type: string }}
     */
    public static getFormats(): Record<
        string,
        { callback: (params: any) => Validator; type: string }
    > {
        return this.formats;
    }

    /**
     * Add a new Validator.
     * Stores a callback and required params to create Validator.
     *
     * @param name - The name of the format
     * @param callback - Callback that accepts params in order and returns Validator
     * @param type - Primitive data type for validation
     */
    public static addFormat(
        name: string,
        callback: (params: any) => Validator,
        type: string,
    ): void {
        this.formats[name] = {
            callback: callback,
            type: type,
        };
    }

    /**
     * Check if validator has been added.
     *
     * @param name - The name of the format
     * @param type - The type of the format
     * @returns {boolean}
     */
    public static hasFormat(name: string, type: string): boolean {
        return (
            (this.formats[name] && this.formats[name].type === type) || false
        );
    }

    /**
     * Get a Format array to create Validator.
     *
     * @param name - The name of the format
     * @param type - The type of the format
     * @returns {array{callback: callable, type: string}}
     * @throws DatabaseException
     */
    public static getFormat(
        name: string,
        type: string,
    ): { callback: (params: any) => Validator; type: string } {
        if (this.formats[name]) {
            if (this.formats[name].type !== type) {
                throw new DatabaseException(
                    `Format "${name}" not available for attribute type "${type}"`,
                );
            }
            return this.formats[name];
        }
        throw new DatabaseException(`Unknown format validator "${name}"`);
    }

    /**
     * Remove a Validator.
     *
     * @param name - The name of the format to remove
     */
    public static removeFormat(name: string): void {
        delete this.formats[name];
    }

    /**
     * Get Description.
     *
     * Returns validator description
     *
     * @returns {string}
     */
    public getDescription(): string {
        return "Invalid document structure: " + this.message;
    }

    protected keys: Record<string, any> = {};

    /**
     * Is valid.
     *
     * Returns true if valid or false if not.
     *
     * @param document - The document to validate
     * @returns {boolean}
     */
    public isValid(document: any): boolean {
        if (!(document instanceof Document)) {
            this.message = "Value must be an instance of Document";
            return false;
        }

        if (!document.getCollection()) {
            this.message = "Missing collection attribute $collection";
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

        if (!this.checkForAllRequiredValues(structure, attributes)) {
            return false;
        }

        if (!this.checkForUnknownAttributes(structure)) {
            return false;
        }

        if (!this.checkForInvalidAttributeValues(structure)) {
            return false;
        }

        return true;
    }

    /**
     * Check for all required values
     *
     * @param structure - The document structure
     * @param attributes - The attributes to validate against
     * @param keys - The list of allowed keys
     * @returns {boolean}
     */
    protected checkForAllRequiredValues(
        structure: Record<string, any>,
        attributes: Record<string, any>[],
    ): boolean {
        for (const attribute of attributes) {
            const name = attribute["$id"] ?? "";
            const required = attribute["required"] ?? false;

            this.keys[name] = attribute;

            if (required && !(name in structure)) {
                this.message = `Missing required attribute "${name}"`;
                return false;
            }
        }

        return true;
    }

    /**
     * Check for Unknown Attributes
     *
     * @param structure - The document structure
     * @param keys - The list of allowed keys
     * @returns {boolean}
     */
    protected checkForUnknownAttributes(
        structure: Record<string, any>,
    ): boolean {
        for (const key in structure) {
            if (!(key in this.keys)) {
                this.message = `Unknown attribute: "${key}"`;
                return false;
            }
        }

        return true;
    }

    /**
     * Check for invalid attribute values
     *
     * @param structure - The document structure
     * @param keys - The list of allowed keys
     * @returns {boolean}
     */
    protected checkForInvalidAttributeValues(
        structure: Record<string, any>,
    ): boolean {
        for (const key in structure) {
            const value = structure[key];
            const attribute = this.keys[key] ?? {};
            const type = attribute["type"] ?? "";
            const array = attribute["array"] ?? false;
            const required = attribute["required"] ?? false;
            const size = attribute["size"] ?? 0;

            if (!required && value === null) {
                continue;
            }

            if (type === Database.VAR_RELATIONSHIP) {
                continue;
            }

            const validators: Validator[] = [];

            switch (type) {
                case Database.VAR_STRING:
                    validators.push(new TextValidator(size, 0));
                    break;

                case Database.VAR_INTEGER:
                    validators.push(new IntegerValidator());
                    const max =
                        size && size >= 8
                            ? Database.BIG_INT_MAX
                            : Database.INT_MAX;
                    const min = attribute["signed"] ? -max : 0;
                    validators.push(new RangeValidator(min, max, "integer"));
                    break;

                case Database.VAR_FLOAT:
                    validators.push(new FloatValidator());
                    const floatMin = attribute["signed"]
                        ? -Database.DOUBLE_MAX
                        : 0;
                    validators.push(
                        new RangeValidator(
                            floatMin,
                            Database.DOUBLE_MAX,
                            "float",
                        ),
                    );
                    break;

                case Database.VAR_BOOLEAN:
                    validators.push(new BooleanValidator());
                    break;

                case Database.VAR_DATETIME:
                    validators.push(
                        new DatetimeValidator(
                            this.minAllowedDate,
                            this.maxAllowedDate,
                        ),
                    );
                    break;

                default:
                    this.message = `Unknown attribute type "${type}"`;
                    return false;
            }

            if (array) {
                if (
                    !required &&
                    ((Array.isArray(value) && value.length === 0) ||
                        value === null)
                ) {
                    continue;
                }

                if (!Array.isArray(value)) {
                    this.message = `Attribute "${key}" must be an array`;
                    return false;
                }

                for (const [index, child] of value.entries()) {
                    if (!required && child === null) {
                        continue;
                    }

                    for (const validator of validators) {
                        if (!validator.isValid(child)) {
                            this.message = `Attribute "${key}['${index}']" has invalid value. ${validator.getDescription()}`;
                            return false;
                        }
                    }
                }
            } else {
                for (const validator of validators) {
                    if (!validator.isValid(value)) {
                        this.message = `Attribute "${key}" has invalid value. ${validator.getDescription()}`;
                        return false;
                    }
                }
            }
        }

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
        return "array"; // Assuming you want to return a string representation of the type
    }
}
