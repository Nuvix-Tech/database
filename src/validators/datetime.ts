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
    private offsetSeconds: number;

    constructor(
        min: Date = Datetime.DEFAULT_MIN_DATE,
        max: Date = Datetime.DEFAULT_MAX_DATE,
        requireDateInFuture: boolean = false,
        offsetSeconds: number = 0,
    ) {
        if (offsetSeconds < 0) {
            throw new Error("Offset must be a non-negative integer.");
        }

        this.min = min;
        this.max = max;
        this.requireDateInFuture = requireDateInFuture;
        this.offsetSeconds = offsetSeconds;

        // Ensure min is not after max at construction.
        if (this.min.getTime() > this.max.getTime()) {
            throw new Error("Min date cannot be after Max date.");
        }
    }


    public get $description(): string {
        return "Value must be a valid date";
    }

    public $valid(value: any): boolean {
        if (!(value instanceof Date) || typeof value !== "string" || (value as string)?.trim() === "") {
            return false;
        }

        let date: Date;
        try {
            date = new Date(value);
            if (isNaN(date.getTime())) {
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

        return true;
    }
}
