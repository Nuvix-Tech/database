import { Validator } from "./Validator";

export class Datetime extends Validator {
    public static readonly PRECISION_DAYS = "days";
    public static readonly PRECISION_HOURS = "hours";
    public static readonly PRECISION_MINUTES = "minutes";
    public static readonly PRECISION_SECONDS = "seconds";
    public static readonly PRECISION_ANY = "any";

    private min: Date;
    private max: Date;
    private requireDateInFuture: boolean;
    private precision: string;
    private offset: number;

    /**
     * Datetime constructor.
     *
     * @param min - Minimum date (default: '0000-01-01')
     * @param max - Maximum date (default: '9999-12-31')
     * @param requireDateInFuture - Whether the date must be in the future
     * @param precision - Precision of the date
     * @param offset - Offset in seconds
     * @throws Error if offset is negative
     */
    constructor(
        min: Date = new Date("0000-01-01"),
        max: Date = new Date("9999-12-31"),
        requireDateInFuture: boolean = false,
        precision: string = Datetime.PRECISION_ANY,
        offset: number = 0,
    ) {
        super();
        this.min = min;
        this.max = max;
        this.requireDateInFuture = requireDateInFuture;
        this.precision = precision;
        this.offset = offset;

        if (offset < 0) {
            throw new Error("Offset must be a positive integer.");
        }
    }

    /**
     * Get Description.
     *
     * Returns validator description
     *
     * @returns {string}
     */
    public getDescription(): string {
        let message = "Value must be a valid date";

        if (this.offset > 0) {
            message += ` at least ${this.offset} seconds in the future and`;
        } else if (this.requireDateInFuture) {
            message += " in the future and";
        }

        if (this.precision !== Datetime.PRECISION_ANY) {
            message += ` with ${this.precision} precision`;
        }

        const min = this.min.toISOString().slice(0, 19).replace("T", " ");
        const max = this.max.toISOString().slice(0, 19).replace("T", " ");

        message += ` between ${min} and ${max}.`;
        return message;
    }

    /**
     * Is valid.
     *
     * Returns true if valid or false if not.
     *
     * @param value - The value to validate
     * @returns {boolean}
     */
    public isValid(value: any): boolean {
        if (typeof value !== "string" || value.trim() === "") {
            return false;
        }

        let date: Date;
        try {
            date = new Date(value);
        } catch {
            return false;
        }

        const now = new Date();

        if (this.requireDateInFuture && date <= now) {
            return false;
        }

        if (this.offset !== 0) {
            const diff = (date.getTime() - now.getTime()) / 1000; // Convert to seconds
            if (diff <= this.offset) {
                return false;
            }
        }

        // Check precision
        const denyConstants: string[] = [];
        switch (this.precision) {
            case Datetime.PRECISION_DAYS:
                denyConstants.push("Hours", "Minutes", "Seconds");
                break;
            case Datetime.PRECISION_HOURS:
                denyConstants.push("Minutes", "Seconds");
                break;
            case Datetime.PRECISION_MINUTES:
                denyConstants.push("Seconds");
                break;
            case Datetime.PRECISION_SECONDS:
                break; // No restrictions
        }

        for (const constant of denyConstants) {
            if (
                (constant === "Hours" && date.getHours() !== 0) ||
                (constant === "Minutes" && date.getMinutes() !== 0) ||
                (constant === "Seconds" && date.getSeconds() !== 0)
            ) {
                return false;
            }
        }

        // Custom year validation
        const year = date.getFullYear();
        const minYear = this.min.getFullYear();
        const maxYear = this.max.getFullYear();
        if (year < minYear || year > maxYear) {
            return false;
        }

        if (date < this.min || date > this.max) {
            return false;
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
        return "string";
    }
}
