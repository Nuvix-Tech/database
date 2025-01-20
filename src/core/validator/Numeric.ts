import { Validator } from './Validator';

/**
 * Numeric
 *
 * Validate that a variable is numeric
 */
export class Numeric extends Validator {
  /**
   * Get Description
   *
   * Returns validator description
   *
   * @returns {string}
   */
  public getDescription(): string {
    return 'Value must be a valid number';
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
    return 'mixed'; // Assuming TYPE_MIXED is equivalent to 'mixed'
  }

  /**
   * Is valid
   *
   * Validation will pass when $value is numeric.
   *
   * @param value - The value to validate
   * @returns {boolean}
   */
  public isValid(value: any): boolean {
    return typeof value === 'number' || !isNaN(Number(value));
  }
}