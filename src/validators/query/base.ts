import { Validator } from "../Validator";

export abstract class Base extends Validator {
    public static readonly METHOD_TYPE_LIMIT = "limit";
    public static readonly METHOD_TYPE_OFFSET = "offset";
    public static readonly METHOD_TYPE_CURSOR = "cursor";
    public static readonly METHOD_TYPE_ORDER = "order";
    public static readonly METHOD_TYPE_FILTER = "filter";
    public static readonly METHOD_TYPE_SELECT = "select";

    protected message: string = "Invalid query";

    /**
     * Get Description.
     *
     * Returns validator description
     *
     * @returns {string}
     */
    public getDescription(): string {
        return this.message;
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
        return "object"; // Assuming TYPE_OBJECT is equivalent to 'object'
    }

    /**
     * Returns what type of query this Validator is for
     */
    abstract getMethodType(): string;
}
