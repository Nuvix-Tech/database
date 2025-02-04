import { DatabaseError as DatabaseException } from "../errors/base";

export class DateTime {
    private static readonly FORMAT_DB: string = "YYYY-MM-DD HH:mm:ss.SSS";
    private static readonly FORMAT_TZ: string = "YYYY-MM-DDTHH:mm:ss.SSSZ";

    private constructor() {}

    /**
     * Get the current date and time formatted as a string.
     * @returns {string}
     */
    public static now(): string {
        return this.format(new Date(), this.FORMAT_DB);
    }

    /**
     * Format a Date object as a string based on the given format.
     * @param {Date} date
     * @param {string} format
     * @returns {string}
     */
    public static format(date: Date, format: string = this.FORMAT_TZ): string {
        if (format === this.FORMAT_DB) {
            return (
                date.getFullYear() +
                "-" +
                String(date.getMonth() + 1).padStart(2, "0") +
                "-" +
                String(date.getDate()).padStart(2, "0") +
                " " +
                String(date.getHours()).padStart(2, "0") +
                ":" +
                String(date.getMinutes()).padStart(2, "0") +
                ":" +
                String(date.getSeconds()).padStart(2, "0") +
                "." +
                String(date.getMilliseconds()).padStart(3, "0")
            );
        }
        return date.toISOString();
    }

    /**
     * Add seconds to a Date object and return the formatted string.
     * @param {Date} date
     * @param {number} seconds
     * @returns {string}
     * @throws {DatabaseException}
     */
    public static addSeconds(date: Date, seconds: number): string {
        if (!Number.isFinite(seconds)) {
            throw new DatabaseException(
                "Invalid interval: seconds must be a finite number.",
            );
        }

        date.setSeconds(date.getSeconds() + seconds);
        return this.format(date, this.FORMAT_DB);
    }

    /**
     * Convert a datetime string to local timezone format.
     * @param {string} datetime
     * @returns {string}
     * @throws {DatabaseException}
     */
    public static setTimezone(datetime: string): string {
        if (!datetime) {
            throw new DatabaseException("Invalid datetime input.");
        }

        try {
            const date = new Date(datetime);
            const offset = date.getTimezoneOffset() * 60000;
            return this.format(
                new Date(date.getTime() - offset),
                this.FORMAT_DB,
            );
        } catch (error: any) {
            throw new DatabaseException(
                `Failed to set timezone: ${error.message}`,
            );
        }
    }

    /**
     * Format a database datetime string with timezone information.
     * @param {string | null} dbFormat
     * @returns {string | null}
     */
    public static formatTz(dbFormat: string | null): string | null {
        if (!dbFormat || this.isInvalidDate(dbFormat)) {
            return null;
        }

        try {
            return this.format(new Date(dbFormat), this.FORMAT_TZ);
        } catch {
            return dbFormat; // Return original if parsing fails
        }
    }

    /**
     * Check if the given datetime string is invalid.
     * @param {string} dateString
     * @returns {boolean}
     */
    private static isInvalidDate(dateString: string): boolean {
        const invalidDates = new Set([
            "0000-00-00 00:00:00",
            "0000-00-00",
            "00:00:00",
            "0000-00-00 00:00:00.000000",
            "0000-00-00 00:00:00.000",
            "0000-00-00 00:00:00.000000+00:00",
        ]);
        return invalidDates.has(dateString);
    }
}
