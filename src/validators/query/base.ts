import { Validator } from "@validators/interface.js";

export enum MethodType {
  Limit = "limit",
  Offset = "offset",
  Cursor = "cursor",
  Order = "order",
  Filter = "filter",
  Select = "select",
  Populate = "populate",
}

export abstract class Base implements Validator {
  protected message: string = "Invalid query";

  /**
   * Get Description.
   *
   * Returns validator description
   *
   * @returns {string}
   */
  public get $description(): string {
    return this.message;
  }

  abstract $valid(query: unknown): boolean;

  /**
   * Returns what type of query this Validator is for
   */
  abstract getMethodType(): string;
}
