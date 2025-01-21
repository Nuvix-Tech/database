import { Base } from "./Base";
import { Query } from "../../query";
import { Document } from "../../Document";
import { Datetime as DatetimeValidator } from "../Datetime";
import { Numeric } from "../Numeric";
import { TextValidator } from "..";

export class Filter extends Base {
    protected schema: Record<string, any> = {};
    protected maxValuesCount: number;
    protected minAllowedDate: Date;
    protected maxAllowedDate: Date;

    constructor(
        attributes: Document[] = [],
        maxValuesCount: number = 100,
        minAllowedDate: Date = new Date("0000-01-01"),
        maxAllowedDate: Date = new Date("9999-12-31"),
    ) {
        super();
        this.maxValuesCount = maxValuesCount;
        this.minAllowedDate = minAllowedDate;
        this.maxAllowedDate = maxAllowedDate;

        for (const attribute of attributes) {
            this.schema[
                attribute.getAttribute("key", attribute.getAttribute("$id"))
            ] = attribute.getArrayCopy();
        }
    }

    protected isValidAttribute(attribute: string): boolean {
        if (attribute.includes(".")) {
            if (this.schema[attribute]) {
                return true;
            }
            attribute = attribute.split(".")[0];
            if (this.schema[attribute]) {
                this.message = "Cannot query nested attribute on: " + attribute;
                return false;
            }
        }

        if (!this.schema[attribute]) {
            this.message = "Attribute not found in schema: " + attribute;
            return false;
        }

        return true;
    }

    protected isValidAttributeAndValues(
        attribute: string,
        values: any[],
        method: string,
    ): boolean {
        if (!this.isValidAttribute(attribute)) {
            return false;
        }

        if (this.schema[attribute].type === "relationship") {
            // Additional relationship validation logic can be added here
        }

        if (values.length > this.maxValuesCount) {
            this.message =
                "Query on attribute has greater than " +
                this.maxValuesCount +
                " values: " +
                attribute;
            return false;
        }

        const attributeType = this.schema[attribute].type;

        for (const value of values) {
            let validator: any;

            switch (attributeType) {
                case "string":
                    validator = new TextValidator(0, 0);
                    break;
                case "integer":
                    validator = new Numeric();
                    break;
                case "float":
                    validator = new Numeric();
                    break;
                case "boolean":
                    validator = new Boolean();
                    break;
                case "datetime":
                    validator = new DatetimeValidator(
                        this.minAllowedDate,
                        this.maxAllowedDate,
                    );
                    break;
                default:
                    this.message = "Unknown Data type";
                    return false;
            }

            if (!validator.isValid(value)) {
                this.message =
                    'Query value is invalid for attribute "' + attribute + '"';
                return false;
            }
        }

        return true;
    }

    protected isEmpty(values: any[]): boolean {
        return (
            values.length === 0 ||
            (Array.isArray(values[0]) && values[0].length === 0)
        );
    }

    public isValid(value: any): boolean {
        const method = value.getMethod();
        const attribute = value.getAttribute();

        switch (method) {
            case Query.TYPE_EQUAL:
            case Query.TYPE_CONTAINS:
                if (this.isEmpty(value.getValues())) {
                    this.message =
                        method.charAt(0).toUpperCase() +
                        method.slice(1) +
                        " queries require at least one value.";
                    return false;
                }
                return this.isValidAttributeAndValues(
                    attribute,
                    value.getValues(),
                    method,
                );

            case Query.TYPE_NOT_EQUAL:
            case Query.TYPE_LESSER:
            case Query.TYPE_LESSER_EQUAL:
            case Query.TYPE_GREATER:
            case Query.TYPE_GREATER_EQUAL:
            case Query.TYPE_SEARCH:
            case Query.TYPE_STARTS_WITH:
            case Query.TYPE_ENDS_WITH:
                if (value.getValues().length !== 1) {
                    this.message =
                        method.charAt(0).toUpperCase() +
                        method.slice(1) +
                        " queries require exactly one value.";
                    return false;
                }
                return this.isValidAttributeAndValues(
                    attribute,
                    value.getValues(),
                    method,
                );

            case Query.TYPE_BETWEEN:
                if (value.getValues().length !== 2) {
                    this.message =
                        method.charAt(0).toUpperCase() +
                        method.slice(1) +
                        " queries require exactly two values.";
                    return false;
                }
                return this.isValidAttributeAndValues(
                    attribute,
                    value.getValues(),
                    method,
                );

            case Query.TYPE_IS_NULL:
            case Query.TYPE_IS_NOT_NULL:
                return this.isValidAttributeAndValues(
                    attribute,
                    value.getValues(),
                    method,
                );

            case Query.TYPE_OR:
            case Query.TYPE_AND:
                const filters = Query.groupByType(value.getValues()).filters;
                if (value.getValues().length !== filters.length) {
                    this.message =
                        method.charAt(0).toUpperCase() +
                        method.slice(1) +
                        " queries can only contain filter queries";
                    return false;
                }
                if (filters.length < 2) {
                    this.message =
                        method.charAt(0).toUpperCase() +
                        method.slice(1) +
                        " queries require at least two queries";
                    return false;
                }
                return true;

            default:
                return false;
        }
    }

    public getMethodType(): string {
        return Base.METHOD_TYPE_FILTER;
    }
}
