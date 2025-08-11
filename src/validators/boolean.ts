import { Validator } from "./interface.js";

export class Boolean implements Validator {
  protected loose: boolean;

  /**
   * Pass true to accept true and false strings and integers 0 and 1 as valid boolean values
   *
   * @param loose - Whether to allow loose validation
   */
  constructor(loose: boolean = false) {
    this.loose = loose;
  }

  public $description: string = "Value must be a valid boolean";

  public $valid(value: any): boolean {
    if (this.loose) {
      if (value === "true" || value === "false" || value === 1 || value === 0) {
        return true;
      }
    }
    return typeof value === "boolean" || value === 1 || value === 0;
  }
}
