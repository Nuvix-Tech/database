import { Validator } from "./interface.js";

export class FloatValidator implements Validator {
  protected loose: boolean;

  /**
   * Pass true to accept float strings as valid float values
   *
   * @param loose - Whether to allow loose validation
   */
  constructor(loose: boolean = false) {
    this.loose = loose;
  }

  public $description: string = "Value must be a valid float";

  public $valid(value: any): boolean {
    if (this.loose) {
      if (typeof value !== "number" && isNaN(Number(value))) {
        return false;
      }
      value = Number(value);
    }

    return typeof value === "number" && !isNaN(value);
  }
}
