import { Base } from "./base.js";
import { Query } from "../../query";
import { Numeric } from "../Numeric";
import { Range } from "../Range";

export class Offset extends Base {
    protected maxOffset: number;

    constructor(maxOffset: number = Number.MAX_SAFE_INTEGER) {
        super();
        this.maxOffset = maxOffset;
    }

    public isValid(value: any): boolean {
        if (!(value instanceof Query)) {
            return false;
        }

        const method = value.getMethod();

        if (method !== Query.TYPE_OFFSET) {
            this.message = "Query method invalid: " + method;
            return false;
        }

        const offset = value.getValue();
        const validator = new Numeric();

        if (!validator.isValid(offset)) {
            this.message = "Invalid offset: " + validator.getDescription();
            return false;
        }

        const rangeValidator = new Range(0, this.maxOffset);
        if (!rangeValidator.isValid(offset)) {
            this.message = "Invalid offset: " + rangeValidator.getDescription();
            return false;
        }

        return true;
    }

    public getMethodType(): string {
        return Base.METHOD_TYPE_OFFSET;
    }
}
