import { Validator } from './Validator'; // Adjust the import based on your project structure

export class Key extends Validator {
  protected allowInternal: boolean; // If true, keys starting with $ are allowed
  protected message: string = "Parameter must contain at most 36 chars. Valid chars are a-z, A-Z, 0-9, period, hyphen, and underscore. Can't start with a special char";

  /**
   * Key constructor.
   *
   * @param allowInternal - Whether to allow internal keys starting with $
   */
  constructor(allowInternal: boolean = false) {
    super();
    this.allowInternal = allowInternal;
  }

  /**
   * Get Description.
   *
   * Returns validator description
   *
   * @returns {string}
   */
  public getDescription(): string {
    return this.message;
  }

  /**
   * Is valid.
   *
   * Returns true if valid or false if not.
   *
   * @param value - The value to validate
   * @returns {boolean}
   */
  public isValid(value: any): boolean {
    if (typeof value !== 'string') {
      return false;
    }

    if (value === '') {
      return false;
    }

    // No leading special characters
    const leading = value.charAt(0);
    if (leading === '_' || leading === '.' || leading === '-') {
      return false;
    }

    const isInternal = leading === '$';

    if (isInternal && !this.allowInternal) {
      return false;
    }

    if (isInternal) {
      const allowList = ['$id', '$createdAt', '$updatedAt'];

      // If exact match, no need for any further checks
      return allowList.includes(value);
    }

    // Valid chars: A-Z, a-z, 0-9, underscore, hyphen, period
    if (/[^A-Za-z0-9_.-]/.test(value)) {
      return false;
    }

    if (value.length > 36) {
      return false;
    }

    return true;
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
    return 'string'; // Assuming you want to return a string representation of the type
  }
}