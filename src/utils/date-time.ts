export class DateTime {
    static readonly FORMAT_DB = "YYYY-MM-DD HH:mm:ss.SSS";
    static readonly FORMAT_ISO = "YYYY-MM-DDTHH:mm:ss.SSSZ";

    private constructor() { }

    public static now(): string {
        return this.formatForDB(new Date()) as string;
    }

    public static formatForDB(date: Date | null | undefined): string | null {
        if (!(date instanceof Date) || isNaN(date.getTime())) return null;

        const year = date.getUTCFullYear();
        const month = String(date.getUTCMonth() + 1).padStart(2, "0");
        const day = String(date.getUTCDate()).padStart(2, "0");
        const hours = String(date.getUTCHours()).padStart(2, "0");
        const minutes = String(date.getUTCMinutes()).padStart(2, "0");
        const seconds = String(date.getUTCSeconds()).padStart(2, "0");
        const milliseconds = String(date.getUTCMilliseconds()).padStart(3, "0");

        return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}.${milliseconds}`;
    }

    public static formatToISO(date: Date | null | undefined): string | null {
        if (!(date instanceof Date) || isNaN(date.getTime())) return null;
        return date.toISOString();
    }

    public static fromDB(dbString: string | null | undefined): Date | null {
        if (typeof dbString !== "string" || dbString.trim() === "") return null;

        const isoString = dbString.replace(" ", "T") + "Z";
        const date = new Date(isoString);

        if (isNaN(date.getTime())) return null;
        return date;
    }

    public static addSeconds(date: Date | null | undefined, seconds: number): string | null {
        if (!(date instanceof Date) || isNaN(date.getTime()) || !Number.isFinite(seconds)) {
            return null;
        }

        date.setSeconds(date.getSeconds() + seconds);
        return this.formatForDB(date);
    }

    public static toLocal(utcString: string | null | undefined): string | null {
        const date = this.fromDB(utcString);
        if (!date) return null;

        const year = date.getFullYear();
        const month = String(date.getMonth() + 1).padStart(2, "0");
        const day = String(date.getDate()).padStart(2, "0");
        const hours = String(date.getHours()).padStart(2, "0");
        const minutes = String(date.getMinutes()).padStart(2, "0");
        const seconds = String(date.getSeconds()).padStart(2, "0");
        const milliseconds = String(date.getMilliseconds()).padStart(3, "0");

        return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}.${milliseconds}`;
    }
}
