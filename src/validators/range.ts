import { Validator } from "./interface.js";
import { Numeric, NumericType } from "./numeric.js";

export class Range extends Numeric implements Validator {
    private min: number;
    private max: number;
    private format: NumericType;

    /**
     * @param min - The minimum allowed value (inclusive).
     * @param max - The maximum allowed value (inclusive).
     * @param format - The expected numeric type (integer or float). Defaults to NumericType.INTEGER.
     * @throws Error if min is greater than max.
     */
    constructor(min: number, max: number, format: NumericType = NumericType.INTEGER) {
        super();
        if (min > max) {
            throw new Error("Minimum value cannot be greater than maximum value.");
        }

        if (!Object.values(NumericType).includes(format)) {
            throw new Error(`Invalid format provided: ${format}. Must be one of ${Object.values(NumericType).join(', ')}.`);
        }

        this.min = min;
        this.max = max;
        this.format = format;
    }

    /**
     * Get Range Minimum Value
     */
    public get $min(): number {
        return this.min;
    }

    /**
     * Get Range Maximum Value
     */
    public get $max(): number {
        return this.max;
    }

    /**
     * Get Range Format
     */
    public get $format(): NumericType {
        return this.format;
    }

    public override get $description(): string {
        const formatter = new Intl.NumberFormat('en-US', {
            minimumFractionDigits: this.format === NumericType.FLOAT ? 1 : 0,
            maximumFractionDigits: this.format === NumericType.FLOAT ? 20 : 0,
        });

        return `Value must be a valid ${this.format} between ${formatter.format(this.min)} and ${formatter.format(this.max)}.`;
    }

    public $valid(value: any): boolean {
        if (!super.$valid(value)) {
            return false;
        }

        if (value === Infinity || value === -Infinity) {
            // If the format is integer, and min/max allow infinity, it's valid.
            // If the format is float, infinity is always a float value.
            return (value >= this.min && value <= this.max);
        }

        switch (this.format) {
            case NumericType.INTEGER:
                if (!Number.isInteger(value)) {
                    return false;
                }
                break;
            case NumericType.FLOAT:
                // `typeof value === 'number'` already handles basic float check.
                break;
            default:
                return false;
        }

        return value >= this.min && value <= this.max;
    }
}
