import { Validator } from "./interface.js";

export enum DatetimePrecision {
    DAYS = "days",
    HOURS = "hours",
    MINUTES = "minutes",
    SECONDS = "seconds",
    ANY = "any",
}

export class Datetime implements Validator {
    private static readonly DEFAULT_MIN_DATE = new Date("0000-01-01T00:00:00.000Z");
    private static readonly DEFAULT_MAX_DATE = new Date("9999-12-31T23:59:59.999Z");

    private min: Date;
    private max: Date;
    private requireDateInFuture: boolean;
    private precision: DatetimePrecision;
    private offsetSeconds: number;

    /**
     * Datetime constructor.
     *
     * @param min - Minimum date (default: '0000-01-01')
     * @param max - Maximum date (default: '9999-12-31')
     * @param requireDateInFuture - Whether the date must be in the future
     * @param precision - Precision of the date (defaults to DatetimePrecision.ANY)
     * @param offsetSeconds - Offset in seconds (must be non-negative)
     * @throws Error if offsetSeconds is negative
     */
    constructor(
        min: Date = Datetime.DEFAULT_MIN_DATE,
        max: Date = Datetime.DEFAULT_MAX_DATE,
        requireDateInFuture: boolean = false,
        precision: DatetimePrecision = DatetimePrecision.ANY,
        offsetSeconds: number = 0,
    ) {
        if (offsetSeconds < 0) {
            throw new Error("Offset must be a non-negative integer.");
        }

        this.min = min;
        this.max = max;
        this.requireDateInFuture = requireDateInFuture;
        this.precision = precision;
        this.offsetSeconds = offsetSeconds;

        // Ensure min is not after max at construction.
        if (this.min.getTime() > this.max.getTime()) {
            throw new Error("Min date cannot be after Max date.");
        }
    }


    public get $description(): string {
        let message = "Value must be a valid date";

        if (this.offsetSeconds > 0) {
            message += ` at least ${this.offsetSeconds} seconds in the future`;
        } else if (this.requireDateInFuture) {
            message += " in the future";
        }

        if ((this.offsetSeconds > 0 || this.requireDateInFuture) && this.precision !== DatetimePrecision.ANY) {
            message += " and";
        } else if (this.precision !== DatetimePrecision.ANY) {
            message += " with";
        }

        if (this.precision !== DatetimePrecision.ANY) {
            message += ` ${this.precision} precision`;
        }

        const formatOptions: Intl.DateTimeFormatOptions = {
            year: 'numeric',
            month: '2-digit',
            day: '2-digit',
            hour: '2-digit',
            minute: '2-digit',
            second: '2-digit',
            hour12: false, // Use 24-hour format
            timeZone: 'UTC', // Ensure consistent formatting regardless of local timezone
        };

        const formattedMin = this.min.toLocaleString('en-US', formatOptions).replace(',', '');
        const formattedMax = this.max.toLocaleString('en-US', formatOptions).replace(',', '');

        if (message.length > "Value must be a valid date".length) {
            message += ` between ${formattedMin} and ${formattedMax}.`;
        } else {
            message += ` between ${formattedMin} and ${formattedMax}.`;
        }

        return message;
    }

    public $valid(value: any): boolean {
        if (typeof value !== "string" || value.trim() === "") {
            return false;
        }

        let date: Date;
        try {
            date = new Date(value);
            if (isNaN(date.getTime())) { // Check if date is "Invalid Date"
                return false;
            }
        } catch (e) {
            return false;
        }

        const now = new Date();
        const requiredFutureDate = new Date(now.getTime() + this.offsetSeconds * 1000);

        if (this.requireDateInFuture && date.getTime() <= now.getTime()) {
            return false;
        }

        if (this.offsetSeconds > 0 && date.getTime() <= requiredFutureDate.getTime()) {
            return false;
        }

        if (date.getTime() < this.min.getTime() || date.getTime() > this.max.getTime()) {
            return false;
        }

        switch (this.precision) {
            case DatetimePrecision.DAYS:
                if (date.getHours() !== 0 || date.getMinutes() !== 0 || date.getSeconds() !== 0 || date.getMilliseconds() !== 0) {
                    return false;
                }
                break;
            case DatetimePrecision.HOURS:
                if (date.getMinutes() !== 0 || date.getSeconds() !== 0 || date.getMilliseconds() !== 0) {
                    return false;
                }
                break;
            case DatetimePrecision.MINUTES:
                if (date.getSeconds() !== 0 || date.getMilliseconds() !== 0) {
                    return false;
                }
                break;
            case DatetimePrecision.SECONDS:
                if (date.getMilliseconds() !== 0) {
                    return false;
                }
                break;
            case DatetimePrecision.ANY:
                // No precision restrictions
                break;
        }

        return true;
    }
}
