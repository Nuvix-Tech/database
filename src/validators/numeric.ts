import { Validator } from "./interface.js";

export class Numeric implements Validator {
  public get $description(): string {
    return "Value must be a valid number";
  }

  public $valid(value: any) {
    return typeof value === "number" || !isNaN(Number(value));
  }
}

export enum NumericType {
  INTEGER = "integer",
  FLOAT = "float",
}
