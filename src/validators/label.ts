import { Key } from "./key.js";

export class Label extends Key {
  protected static override MESSAGE: string =
    "Value must be a valid string between 1 and 36 chars containing only alphanumeric chars";

  public $valid(value: any): boolean {
    if (!super.$valid(value)) {
      return false;
    }

    if (/[^A-Za-z0-9]/.test(value)) {
      return false;
    }

    return true;
  }
}
