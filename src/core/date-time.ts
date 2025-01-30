import { DatabaseError as DatabaseException } from "../errors/base";

export class DateTime {
    protected static formatDb: string = "YYYY-MM-DD HH:mm:ss.SSS";
    protected static _formatTz: string = "YYYY-MM-DDTHH:mm:ss.SSSZ";

    private constructor() {}

    /**
     * Get the current date and time formatted as a string
     *
     * @returns {string}
     */
    public static now(): string {
        const date = new Date();
        return this.format(date);
    }

    /**
     * Format a Date object as a string
     *
     * @param {Date} date
     * @returns {string}
     */
    public static format(date: Date): string {
        return date.toISOString();
    }

    /**
     * Add seconds to a Date object and return the formatted string
     *
     * @param {Date} date
     * @param {number} seconds
     * @returns {string}
     * @throws {DatabaseException}
     */
    public static addSeconds(date: Date, seconds: number): string {
        if (isNaN(seconds)) {
            throw new DatabaseException("Invalid interval");
        }

        date.setSeconds(date.getSeconds() + seconds);
        return this.format(date);
    }

    /**
     * Set the timezone of a datetime string and return the formatted string
     *
     * @param {string} datetime
     * @returns {string}
     * @throws {DatabaseException}
     */
    public static setTimezone(datetime: string): string {
        try {
            const value = new Date(datetime);
            const offset = value.getTimezoneOffset() * 60000;
            const localISOTime = new Date(value.getTime() - offset)
                .toISOString()
                .slice(0, -1);
            return localISOTime;
        } catch (e: any) {
            throw new DatabaseException(e.message);
        }
    }

    /**
     * Format a datetime string with timezone information
     *
     * @param {string | null} dbFormat
     * @returns {string | null}
     */
    public static formatTz(dbFormat: string | null): string | null {
        if (
            dbFormat === null ||
            dbFormat === "" ||
            dbFormat === "0000-00-00 00:00:00" ||
            dbFormat === "0000-00-00" ||
            dbFormat === "00:00:00" ||
            dbFormat === "0000-00-00 00:00:00.000000" ||
            dbFormat === "0000-00-00 00:00:00.000" ||
            dbFormat === "0000-00-00 00:00:00.000000+00:00" ||
            dbFormat === undefined
        ) {
            return null;
        }

        try {
            const value = new Date(dbFormat);
            return value.toISOString(); // This will include timezone information
        } catch {
            return dbFormat; // Return the original string if parsing fails
        }
    }
}
