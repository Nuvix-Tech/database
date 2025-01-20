import { Validator } from './Validator';

/**
 * FloatValidator
 *
 * Validate that a variable is a float
 */
export class FloatValidator extends Validator {
  protected loose: boolean;

  /**
   * Pass true to accept float strings as valid float values
   * This option is good for validating query string params.
   *
   * @param loose - Whether to allow loose validation
   */
  constructor(loose: boolean = false) {
    super();
    this.loose = loose;
  }

  /**
   * Get Description
   *
   * Returns validator description
   *
   * @returns {string}
   */
  public getDescription(): string {
    return 'Value must be a valid float';
  }

  /**
   * Is array
   *
   * Function will return true if object is array.
   *
   * @returns {boolean}
   */
  public isArray(): boolean {
    return false;
  }

  /**
   * Get Type
   *
   * Returns validator type.
   *
   * @returns {string}
   */
  public getType(): string {
    return 'float'; // Assuming TYPE_FLOAT is equivalent to 'float'
  }

  /**
   * Is valid
   *
   * Validation will pass when $value is float.
   *
   * @param value - The value to validate
   * @returns {boolean}
   */
  public isValid(value: any): boolean {
    if (this.loose) {
      if (typeof value !== 'number' && isNaN(Number(value))) {
        return false;
      }
      value = Number(value);
    }

    return typeof value === 'number' && !isNaN(value);
  }
}