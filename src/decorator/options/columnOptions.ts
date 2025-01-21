export enum ColumnType {
  STRING = "string",
  BOOLEAN = "boolean",
  INTEGER = "integer",
  FLOAT = "double",
  DATETIME = 'datetime'
}

/**
 * Interface representing the options for a database column.
 */
export interface ColumnOptions {
  /**
   * The name of the column.
   */
  name?: string;

  /**
   * The type of the column.
   */
  type: "string" | "boolean" | "integer" | "datetime" | "varchar" | "text" | "json" | "timestamp";

  /**
   * The size of the column.
   */
  size?: number;

  nullable?: boolean;

  length?: number;

  /**
   * Whether the column is required.
   */
  required?: boolean;

  /**
   * Whether the column is signed.
   */
  signed?: boolean;

  /**
   * Whether the column is an array.
   */
  array?: boolean;

  /**
   * The filters to apply to the column.
   */
  filters?: any[];

  default?: any;
}