import { Numeric } from "./Numeric";

/**
 * Range
 *
 * Validates that a number is in range.
 */
export class Range extends Numeric {
    protected min: number;
    protected max: number;

    /**
     * @param min - Minimum value
     * @param max - Maximum value
     */
    constructor(min: number, max: number, format?: string) {
        super();
        this.min = min;
        this.max = max;
    }

    /**
     * Get Description
     *
     * Returns validator description
     *
     * @returns {string}
     */
    public override getDescription(): string {
        return `Value must be a valid range between ${this.min} and ${this.max}`;
    }

    /**
     * Is valid
     *
     * Validation will pass when $value is within the defined range.
     *
     * @param value - The value to validate
     * @returns {boolean}
     */
    public override isValid(value: any): boolean {
        if (!super.isValid(value)) {
            return false;
        }

        const numValue = Number(value);
        return numValue >= this.min && numValue <= this.max;
    }
}
