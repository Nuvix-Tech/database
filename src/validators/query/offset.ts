import { Query, QueryType } from "@core/query.js";
import { Base, MethodType } from "./base.js";
import { Numeric } from "@validators/numeric.js";
import { Range } from "@validators/range.js";

export class Offset extends Base {
    constructor(private readonly maxOffset: number = Number.MAX_SAFE_INTEGER) {
        super();
    }

    public $valid(value: unknown): boolean {
        if (!(value instanceof Query)) {
            this.message = "Value is not a Query instance.";
            return false;
        }

        if (value.getMethod() !== QueryType.Offset) {
            this.message = `Invalid query method: ${value.getMethod()}`;
            return false;
        }

        const offset = value.getValue();
        const numericValidator = new Numeric();

        if (!numericValidator.$valid(offset)) {
            this.message = `Invalid offset: ${numericValidator.$description}`;
            return false;
        }

        const rangeValidator = new Range(0, this.maxOffset);
        if (!rangeValidator.$valid(offset)) {
            this.message = `Offset out of range: ${rangeValidator.$description}`;
            return false;
        }

        return true;
    }

    public getMethodType(): MethodType {
        return MethodType.Offset;
    }
}
