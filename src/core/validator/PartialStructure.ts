import { Structure } from './Structure';
import { Document } from '../Document';
import { Database } from '../database';

export class PartialStructure extends Structure {
  /**
   * Is valid.
   *
   * Returns true if valid or false if not.
   *
   * @param document - The document to validate
   * @returns {boolean}
   */
  public isValid(document: any): boolean {
    if (!(document instanceof Document)) {
      this.message = 'Value must be an instance of Document';
      return false;
    }

    if (!this.collection.getId() || this.collection.getCollection() !== Database.METADATA) {
      this.message = 'Collection not found';
      return false;
    }

    const keys: Record<string, any> = {};
    const structure = document.getArrayCopy();
    const attributes = { ...this.attributes, ...this.collection.getAttribute('attributes', []) };

    for (const attribute of attributes) {
      const name = attribute['$id'] ?? '';
      keys[name] = attribute;
    }

    if (!this.checkForUnknownAttributes(structure, keys)) {
      return false;
    }

    if (!this.checkForInvalidAttributeValues(structure, keys)) {
      return false;
    }

    return true;
  }
}