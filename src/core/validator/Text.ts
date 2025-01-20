import { Validator } from './Validator';
/**
 * Text
 *
 * Validate that a variable is a valid text value
 */
export class Text extends Validator {
  protected length: number;
  protected min: number;
  protected allowList: string[];

  /**
   * Text constructor.
   *
   * @param length - Maximum length of the text
   * @param min - Minimum length of the text
   * @param allowList - Allowed characters
   */
  constructor(length: number, min: number = 1, allowList: string[] = []) {
    super();
    this.length = length;
    this.min = min;
    this.allowList = allowList;
  }

  /**
   * Get Description
   *
   * Returns validator description
   *
   * @returns {string}
   */
  public getDescription(): string {
    let message = 'Value must be a valid string';

    if (this.min) {
      message += ` and at least ${this.min} chars`;
    }

    if (this.length) {
      message += ` and no longer than ${this.length} chars`;
    }

    if (this.allowList.length > 0) {
      message += ` and only consist of '${this.allowList.join(', ')}' chars`;
    }

    return message;
  }

  /**
   * Is valid
   *
   * Validation will pass when $value is text with valid length.
   *
   * @param value - The value to validate
   * @returns {boolean}
   */
  public isValid(value: any): boolean {
    if (typeof value !== 'string') {
      return false;
    }

    if (value.length < this.min || (this.length > 0 && value.length > this.length)) {
      return false;
    }

    if (this.allowList.length > 0) {
      for (const char of value) if (!this.allowList.includes(char)) {
        return false;
      }
    }

    return true;
  }
}