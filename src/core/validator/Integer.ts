import { Validator } from "./Validator";

/**
 * Integer
 *
 * Validate that a variable is an integer
 */
export class Integer extends Validator {
    protected loose: boolean;

    /**
     * Pass true to accept integer strings as valid integer values
     *
     * @param loose - Whether to allow loose validation
     */
    constructor(loose: boolean = false) {
        super();
        this.loose = loose;
    }

    /**
     * Get Description
     *
     * Returns validator description
     *
     * @returns {string}
     */
    public getDescription(): string {
        return "Value must be a valid integer";
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
        return "integer"; // Assuming TYPE_INTEGER is equivalent to 'integer'
    }

    /**
     * Is valid
     *
     * Validation will pass when $value is an integer.
     *
     * @param value - The value to validate
     * @returns {boolean}
     */
    public isValid(value: any): boolean {
        if (this.loose) {
            if (typeof value === "string" && !isNaN(Number(value))) {
                value = Number(value);
            }
        }
        return Number.isInteger(value);
    }
}
