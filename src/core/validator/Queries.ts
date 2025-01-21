import { Validator } from "./Validator";
import { Query } from "../query";
import { Base } from "./Query/Base";

export class Queries extends Validator {
    protected message: string = "Invalid queries";
    protected validators: Base[];
    protected length: number;

    /**
     * Queries constructor
     *
     * @param validators - Array of Base validators
     * @param length - Maximum number of queries allowed
     */
    constructor(validators: Base[] = [], length: number = 0) {
        super();
        this.validators = validators;
        this.length = length;
    }

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
     * Is valid.
     *
     * Returns true if valid or false if not.
     *
     * @param value - The value to validate
     * @returns {boolean}
     */
    public isValid(value: any): boolean {
        if (!Array.isArray(value)) {
            this.message = "Queries must be an array";
            return false;
        }

        if (this.length && value.length > this.length) {
            this.message = `Too many queries, maximum allowed is ${this.length}`;
            return false;
        }

        for (const query of value) {
            let parsedQuery: Query;

            if (!(query instanceof Query)) {
                try {
                    parsedQuery = Query.parse(query);
                } catch (error) {
                    this.message = "Invalid query: " + (error as Error).message;
                    return false;
                }
            } else {
                parsedQuery = query;
            }

            if (parsedQuery.isNested()) {
                if (!this.isValid(parsedQuery.getValues())) {
                    return false;
                }
            }

            const method = parsedQuery.getMethod();
            const methodType = this.getMethodType(method);

            let methodIsValid = false;
            for (const validator of this.validators) {
                if (validator.getMethodType() !== methodType) {
                    continue;
                }
                if (!validator.isValid(parsedQuery)) {
                    this.message =
                        "Invalid query: " + validator.getDescription();
                    return false;
                }

                methodIsValid = true;
            }

            if (!methodIsValid) {
                this.message = "Invalid query method: " + method;
                return false;
            }
        }

        return true;
    }

    /**
     * Get the method type based on the query method
     *
     * @param method - The query method
     * @returns {string}
     */
    private getMethodType(method: string): string {
        switch (method) {
            case Query.TYPE_SELECT:
                return Base.METHOD_TYPE_SELECT;
            case Query.TYPE_LIMIT:
                return Base.METHOD_TYPE_LIMIT;
            case Query.TYPE_OFFSET:
                return Base.METHOD_TYPE_OFFSET;
            case Query.TYPE_CURSOR_AFTER:
            case Query.TYPE_CURSOR_BEFORE:
                return Base.METHOD_TYPE_CURSOR;
            case Query.TYPE_ORDER_ASC:
            case Query.TYPE_ORDER_DESC:
                return Base.METHOD_TYPE_ORDER;
            case Query.TYPE_EQUAL:
            case Query.TYPE_NOT_EQUAL:
            case Query.TYPE_LESSER:
            case Query.TYPE_LESSER_EQUAL:
            case Query.TYPE_GREATER:
            case Query.TYPE_GREATER_EQUAL:
            case Query.TYPE_SEARCH:
            case Query.TYPE_IS_NULL:
            case Query.TYPE_IS_NOT_NULL:
            case Query.TYPE_BETWEEN:
            case Query.TYPE_STARTS_WITH:
            case Query.TYPE_CONTAINS:
            case Query.TYPE_ENDS_WITH:
            case Query.TYPE_AND:
            case Query.TYPE_OR:
                return Base.METHOD_TYPE_FILTER;
            default:
                return "";
        }
    }

    /**
     * Is array
     *
     * Function will return true if object is array.
     *
     * @returns {boolean}
     */
    public isArray(): boolean {
        return true;
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
}
