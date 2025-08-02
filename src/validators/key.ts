import { Validator } from "./interface.js";

export class Key implements Validator {
    protected allowInternal: boolean;
    private static readonly MAX_LENGTH: number = 36;
    protected static readonly MESSAGE: string =
        "Parameter must contain at most 36 chars. Valid chars are a-z, A-Z, 0-9, period, hyphen, and underscore. Can't start with a special char";
    private static readonly ALLOWED_INTERNAL_KEYS: ReadonlySet<string> = new Set([
        "$id",
        "$createdAt",
        "$updatedAt",
    ]);

    constructor(allowInternal: boolean = false) {
        this.allowInternal = allowInternal;
    }

    public get $description(): string {
        return Key.MESSAGE;
    }

    public $valid(value: any): boolean {
        if (typeof value !== "string" || value === "") {
            return false;
        }

        if (value.length > Key.MAX_LENGTH) {
            return false;
        }

        const leadingChar = value.charAt(0);
        const isInternal = leadingChar === "$";

        if (isInternal) {
            if (!this.allowInternal) {
                return false;
            }
            return Key.ALLOWED_INTERNAL_KEYS.has(value);
        }

        if (leadingChar === "_" || leadingChar === "." || leadingChar === "-") {
            return false;
        }

        return /^[A-Za-z0-9_.-]+$/.test(value);
    }
}
