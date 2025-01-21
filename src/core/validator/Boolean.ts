import { Validator } from './Validator';

/**
 * Boolean
 *
 * Validate that a variable is a boolean value
 */
export class Boolean extends Validator {
  protected loose: boolean;

  /**
   * Pass true to accept true and false strings and integers 0 and 1 as valid boolean values
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
    return 'Value must be a valid boolean';
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
    return 'boolean'; // Assuming TYPE_BOOLEAN is equivalent to 'boolean'
  }

  /**
   * Is valid
   *
   * Validation will pass when $value has a boolean value.
   *
   * @param value - The value to validate
   * @returns {boolean}
   */
  public isValid(value: any): boolean {
    if (this.loose) {
      if (value === 'true' || value === 'false' || value === 1 || value === 0) {
        return true;
      }
    }
    return typeof value === 'boolean' || value === 1 || value === 0;
  }
}