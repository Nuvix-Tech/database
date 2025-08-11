import { Query, QueryType } from "@core/query.js";
import { Validator } from "./interface.js";
import { Base, MethodType } from "./query/base.js";

export class Queries implements Validator {
  protected message: string = "Invalid queries";
  protected validators: Base[];
  protected maxLength: number;

  /**
   * Queries constructor
   *
   * @param validators - Array of Base validators. Each Base validator defines rules for a specific query method type (e.g., filters, orders).
   * @param maxLength - Maximum number of queries allowed. 0 means unlimited.
   * @throws {Error} If maxLength is negative or validators array contains non-Base instances.
   */
  constructor(validators: Base[] = [], maxLength: number = 0) {
    if (maxLength < 0) {
      throw new Error(
        "Maximum length of queries must be a non-negative number.",
      );
    }
    this.maxLength = maxLength;

    if (
      !Array.isArray(validators) ||
      validators.some((v) => !(v instanceof Base))
    ) {
      throw new Error(
        "Validators must be an array containing instances of Base.",
      );
    }
    this.validators = validators;
  }

  /**
   * Get Description.
   * Returns validator description
   * @returns {string}
   */
  public get $description(): string {
    return this.message;
  }

  /**
   * Is valid.
   * Returns true if valid or false if not.
   *
   * @param value - The value to validate, expected to be an array of Query objects or JSON strings.
   * @returns {boolean}
   */
  public $valid(value: unknown): boolean {
    this.message = "Invalid queries";

    if (!Array.isArray(value)) {
      this.message = "Queries must be an array.";
      return false;
    }

    const queriesArray: unknown[] = value;

    if (this.maxLength > 0 && queriesArray.length > this.maxLength) {
      this.message = `Too many queries, maximum allowed is ${this.maxLength}.`;
      return false;
    }

    for (const query of queriesArray) {
      let parsedQuery: Query;

      if (!(query instanceof Query)) {
        if (
          (typeof query !== "string" && typeof query !== "object") ||
          query === null
        ) {
          this.message =
            "Each query must be a Query object, a JSON string, or a plain object.";
          return false;
        }
        try {
          parsedQuery = Query.parse(query);
        } catch (error) {
          this.message = `Invalid query format or structure: ${error instanceof Error ? error.message : "unknown error"}.`;
          return false;
        }
      } else {
        parsedQuery = query;
      }

      if (parsedQuery.isNested()) {
        const nestedQueries = parsedQuery.getValues();
        if (
          !Array.isArray(nestedQueries) ||
          nestedQueries.some((val) => !(val instanceof Query))
        ) {
          this.message = `Nested query values for method "${parsedQuery.getMethod()}" must be an array of Query objects.`;
          return false;
        }
        if (!this.$valid(nestedQueries)) {
          // The recursive call would have set the message.
          return false;
        }
      }

      const method = parsedQuery.getMethod();
      const methodType = this.getMethodType(method);

      if (!methodType) {
        this.message = `Unrecognized query method type for method: "${method}".`;
        return false;
      }

      let methodIsValid = false;
      for (const validator of this.validators) {
        if (validator.getMethodType() !== methodType) {
          continue;
        }

        if (!validator.$valid(parsedQuery)) {
          const attribute = parsedQuery.getAttribute();
          this.message = `Invalid query for "${method}"${attribute && `and attribute "${attribute}"`}: ${validator.$description}`;
          return false;
        }

        methodIsValid = true;
        break;
      }

      if (!methodIsValid) {
        // This message would be set by the failing validator, or if no validator passed.
        // If it's still 'Invalid queries' it means no validator matched the type,
        // or a matched validator's message wasn't specific enough.
        if (this.message === "Invalid queries") {
          this.message = `No valid validator found or query failed for method: "${method}".`;
        }
        return false;
      }
    }

    return true;
  }

  /**
   * Get the method type based on the query method.
   * This mapping helps to categorize query methods for specific validators.
   *
   * @param method - The query method (e.g., QueryType.Equal, QueryType.Select).
   * @returns {MethodType | undefined} The corresponding MethodType, or undefined if not recognized.
   */
  private getMethodType(method: QueryType): MethodType | undefined {
    switch (method) {
      case QueryType.Select:
        return MethodType.Select;
      case QueryType.Limit:
        return MethodType.Limit;
      case QueryType.Offset:
        return MethodType.Offset;
      case QueryType.CursorAfter:
      case QueryType.CursorBefore:
        return MethodType.Cursor;
      case QueryType.OrderAsc:
      case QueryType.OrderDesc:
        return MethodType.Order;
      case QueryType.Equal:
      case QueryType.NotEqual:
      case QueryType.LessThan:
      case QueryType.LessThanEqual:
      case QueryType.GreaterThan:
      case QueryType.GreaterThanEqual:
      case QueryType.Search:
      case QueryType.IsNull:
      case QueryType.IsNotNull:
      case QueryType.Between:
      case QueryType.StartsWith:
      case QueryType.Contains:
      case QueryType.EndsWith:
      case QueryType.And:
      case QueryType.Or:
        return MethodType.Filter;
      case QueryType.Populate:
        return MethodType.Populate;
      default:
        // If a new QueryType is added but not mapped here, it's an unhandled case.
        return undefined;
    }
  }
}
