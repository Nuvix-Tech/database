import { Base } from "./base.js";
import { Query } from "../../query";
import { Document } from "../../Document";
import { Datetime as DatetimeValidator } from "../Datetime";
import { BooleanValidator, IntegerValidator, TextValidator } from "..";
import { FloatValidator } from "../FloatValidator";
import { Constant } from "../../../core/constant";

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
            ] = attribute.toObject();
        }
    }

    protected isValidAttribute(attribute: string): boolean {
        if (attribute.includes(".")) {
            if (this.schema[attribute]) {
                return true;
            }
            attribute = attribute.split(".")[0]!;
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

        if (attribute.includes(".") && !this.schema[attribute]) {
            attribute = attribute.split(".")[0]!;
        }

        const attributeSchema = this.schema[attribute];

        if (values.length > this.maxValuesCount) {
            this.message =
                "Query on attribute has greater than " +
                this.maxValuesCount +
                " values: " +
                attribute;
            return false;
        }

        const attributeType = attributeSchema.type;

        for (const value of values) {
            let validator: any;

            switch (attributeType) {
                case Constant.VAR_STRING:
                    validator = new TextValidator(0, 0);
                    break;
                case Constant.VAR_INTEGER:
                    validator = new IntegerValidator();
                    break;
                case Constant.VAR_FLOAT:
                    validator = new FloatValidator();
                    break;
                case Constant.VAR_BOOLEAN:
                    validator = new BooleanValidator();
                    break;
                case Constant.VAR_DATETIME:
                    validator = new DatetimeValidator(
                        this.minAllowedDate,
                        this.maxAllowedDate,
                    );
                    break;
                case Constant.VAR_RELATIONSHIP:
                    validator = new TextValidator(255, 0);
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

        if (attributeSchema.type === Constant.VAR_RELATIONSHIP) {
            const options = attributeSchema.options;

            if (
                (options.relationType === Constant.RELATION_ONE_TO_ONE &&
                    !options.twoWay &&
                    options.side === Constant.RELATION_SIDE_CHILD) ||
                (options.relationType === Constant.RELATION_ONE_TO_MANY &&
                    options.side === Constant.RELATION_SIDE_PARENT) ||
                (options.relationType === Constant.RELATION_MANY_TO_ONE &&
                    options.side === Constant.RELATION_SIDE_CHILD) ||
                options.relationType === Constant.RELATION_MANY_TO_MANY
            ) {
                this.message = "Cannot query on virtual relationship attribute";
                return false;
            }
        }

        const isArray = attributeSchema.array ?? false;

        if (
            !isArray &&
            method === Query.TYPE_CONTAINS &&
            attributeSchema.type !== Constant.VAR_STRING
        ) {
            this.message =
                'Cannot query contains on attribute "' +
                attribute +
                '" because it is not an array or string.';
            return false;
        }

        if (
            isArray &&
            ![
                Query.TYPE_CONTAINS,
                Query.TYPE_IS_NULL,
                Query.TYPE_IS_NOT_NULL,
            ].includes(method)
        ) {
            this.message =
                "Cannot query " +
                method +
                ' on attribute "' +
                attribute +
                '" because it is an array.';
            return false;
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
