import { FloatValidator } from "./float-validator.js";

export class Integer extends FloatValidator {
  /**
   * Pass true to accept integer strings as valid integer values
   *
   * @param loose - Whether to allow loose validation
   */
  constructor(loose: boolean = false) {
    super();
    this.loose = loose;
  }

  public $description: string = "Value must be a valid integer";

  public $valid(value: any): boolean {
    if (this.loose) {
      if (typeof value === "string" && !isNaN(Number(value))) {
        value = Number(value);
      }
    }
    return Number.isInteger(value);
  }
}
