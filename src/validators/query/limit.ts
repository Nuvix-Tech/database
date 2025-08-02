import { Query, QueryType } from "@core/query.js";
import { Base, MethodType } from "./base.js";
import { Numeric } from "@validators/numeric.js";
import { Range } from "@validators/range.js";

export class Limit extends Base {
    constructor(private readonly maxLimit: number = Number.MAX_SAFE_INTEGER) {
        super();
    }

    public $valid(value: unknown): boolean {
        if (!(value instanceof Query)) {
            this.message = "Value is not a Query instance.";
            return false;
        }

        if (value.getMethod() !== QueryType.Limit) {
            this.message = `Invalid query method: ${value.getMethod()}`;
            return false;
        }

        const limit = value.getValue();
        const numericValidator = new Numeric();

        if (!numericValidator.$valid(limit)) {
            this.message = `Invalid limit: ${numericValidator.$description}`;
            return false;
        }

        const rangeValidator = new Range(1, this.maxLimit);
        if (!rangeValidator.$valid(limit)) {
            this.message = `Limit out of range: ${rangeValidator.$description}`;
            return false;
        }

        return true;
    }

    public getMethodType(): MethodType {
        return MethodType.Limit;
    }
}
