import { Base } from "./base.js";
import { Query } from "../../query";
import { Numeric } from "../Numeric";
import { Range } from "../Range";

export class Limit extends Base {
    protected maxLimit: number;

    constructor(maxLimit: number = Number.MAX_SAFE_INTEGER) {
        super();
        this.maxLimit = maxLimit;
    }

    public isValid(value: any): boolean {
        if (!(value instanceof Query)) {
            return false;
        }

        if (value.getMethod() !== Query.TYPE_LIMIT) {
            this.message = "Invalid query method: " + value.getMethod();
            return false;
        }

        const limit = value.getValue();
        const validator = new Numeric();

        if (!validator.isValid(limit)) {
            this.message = "Invalid limit: " + validator.getDescription();
            return false;
        }

        const rangeValidator = new Range(1, this.maxLimit);
        if (!rangeValidator.isValid(limit)) {
            this.message = "Invalid limit: " + rangeValidator.getDescription();
            return false;
        }

        return true;
    }

    public getMethodType(): string {
        return Base.METHOD_TYPE_LIMIT;
    }
}
