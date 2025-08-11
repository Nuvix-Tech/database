import { Query, QueryType } from "@core/query.js";
import { Base, MethodType } from "./base.js";
import { Doc } from "@core/doc.js";
import { UID } from "@validators/uid.js";

/**
 * Validates Query objects that represent cursor-based pagination (e.g., CursorAfter, CursorBefore).
 * Ensures the cursor value is a valid UID.
 */
export class Cursor extends Base {
  protected message: string = "Invalid cursor query";

  /**
   * Validates if the given value is a valid cursor Query.
   * Checks the query method and the validity of the cursor value.
   *
   * @param value - The value to validate, expected to be a Query instance.
   * @returns {boolean} True if the query is a valid cursor query, false otherwise.
   */
  public $valid(value: unknown): boolean {
    this.message = "Invalid cursor query";

    if (!(value instanceof Query)) {
      this.message = "Value must be a Query object.";
      return false;
    }

    const method = value.getMethod();
    if (method !== QueryType.CursorAfter && method !== QueryType.CursorBefore) {
      this.message = `Query method "${method}" is not a cursor method.`;
      return false;
    }

    let cursor: unknown = value.getValue();
    if (cursor instanceof Query) {
      this.message = "Cursor value cannot be a nested query.";
      return false;
    }

    let normalizedCursor: string | number | null = null;
    if (cursor instanceof Doc) {
      normalizedCursor = cursor.getId();
    } else if (typeof cursor === "string" || typeof cursor === "number") {
      normalizedCursor = cursor;
    } else if (cursor === null) {
      this.message = "Cursor value cannot be null.";
      return false;
    } else {
      this.message = `Invalid type for cursor value: ${typeof cursor}. Expected string, number, or Doc.`;
      return false;
    }

    const uidValidator = new UID();
    if (!uidValidator.$valid(normalizedCursor)) {
      this.message = `Invalid cursor value: ${uidValidator.$description}`;
      return false;
    }

    return true;
  }

  /**
   * Returns the method type handled by this validator.
   * @returns {MethodType.Cursor} The string literal 'cursor'.
   */
  public getMethodType(): MethodType {
    return MethodType.Cursor;
  }
}
