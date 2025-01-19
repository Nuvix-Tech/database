import { DatabaseError } from '../errors/base';
import { Database } from './database';


export class Document extends Map<string, any> {
  public static readonly SET_TYPE_ASSIGN = 'assign';
  public static readonly SET_TYPE_PREPEND = 'prepend';
  public static readonly SET_TYPE_APPEND = 'append';

  /**
   * Construct a new Document object
   *
   * @param input - Initial data for the document
   * @throws DatabaseException
   */
  constructor(input: Record<string, any> = {}) {
    super();

    if (input['$permissions'] && !Array.isArray(input['$permissions'])) {
      throw new DatabaseError('$permissions must be of type array');
    }

    for (const [key, value] of Object.entries(input)) {
      if (Array.isArray(value)) {
        if (value.some(item => item['$id'] || item['$collection'])) {
          this.set(key, new Document(value));
        } else {
          this.set(key, value.map(item => (item['$id'] || item['$collection']) ? new Document(item) : item));
        }
      } else if (value && typeof value === 'object' && (value['$id'] || value['$collection'])) {
        this.set(key, new Document(value));
      } else {
        this.set(key, value);
      }
    }
  }

  public getId(): string {
    return this.getAttribute('$id', '');
  }

  public getInternalId(): string {
    return this.getAttribute('$internalId', '');
  }

  public getCollection(): string {
    return this.getAttribute('$collection', '');
  }

  public getPermissions(): string[] {
    return Array.from(new Set(this.getAttribute('$permissions', [])));
  }

  public getRead(): string[] {
    return this.getPermissionsByType('read');
  }

  public getCreate(): string[] {
    return this.getPermissionsByType('create');
  }

  public getUpdate(): string[] {
    return this.getPermissionsByType('update');
  }

  public getDelete(): string[] {
    return this.getPermissionsByType('delete');
  }

  public getWrite(): string[] {
    return Array.from(new Set([...this.getCreate(), ...this.getUpdate(), ...this.getDelete()]));
  }

  public getPermissionsByType(type: string): string[] {
    return this.getPermissions()
      .filter(permission => permission.startsWith(type))
      .map(permission => permission.replace(`${type}(`, '').replace(')', '').replace(/"/g, '').trim());
  }

  public getCreatedAt(): string | null {
    return this.getAttribute('$createdAt');
  }

  public getUpdatedAt(): string | null {
    return this.getAttribute('$updatedAt');
  }

  public getAttributes(): Record<string, any> {
    const attributes: Record<string, any> = {};
    const internalKeys = Array.from(Database.INTERNAL_ATTRIBUTES).map(attr => (attr as any)['id']);

    for (const [key, value] of this) {
      if (!internalKeys.includes(key)) {
        attributes[key] = value;
      }
    }

    return attributes;
  }

  public getAttribute(name: string, defaultValue: any = null): any {
    return this.has(name) ? this.get(name) : defaultValue;
  }

  public setAttribute(key: string, value: any, type: string = Document.SET_TYPE_ASSIGN): this {
    switch (type) {
      case Document.SET_TYPE_ASSIGN:
        this.set(key, value);
        break;
      case Document.SET_TYPE_APPEND:
        const appendArray = this.get(key) || [];
        this.set(key, Array.isArray(appendArray) ? [...appendArray, value] : [value]);
        break;
      case Document.SET_TYPE_PREPEND:
        const prependArray = this.get(key) || [];
        this.set(key, Array.isArray(prependArray) ? [value, ...prependArray] : [value]);
        break;
    }
    return this;
  }

  public setAttributes(attributes: Record<string, any>): this {
    for (const [key, value] of Object.entries(attributes)) {
      this.setAttribute(key, value);
    }
    return this;
  }

  public removeAttribute(key: string): this {
    this.delete(key);
    return this;
  }

  public find(key: string, find: any, subject: string = ''): any {
    const subjectData = this.get(subject) || this;
    if (Array.isArray(subjectData)) {
      return subjectData.find(value => value[key] === find) || false;
    }
    return this.has(key) && this.get(key) === find ? subjectData : false;
  }

  public findAndReplace(key: string, find: any, replace: any, subject: string = ''): boolean {
    const subjectData = this.get(subject) || this;
    if (Array.isArray(subjectData)) {
      for (let i = 0; i < subjectData.length; i++) {
        if (subjectData[i][key] === find) {
          subjectData[i] = replace;
          return true;
        }
      }
      return false;
    }
    if (this.has(key) && this.get(key) === find) {
      this.set(key, replace);
      return true;
    }
    return false;
  }

  public findAndRemove(key: string, find: any, subject: string = ''): boolean {
    const subjectData = this.get(subject) || this;
    if (Array.isArray(subjectData)) {
      for (let i = 0; i < subjectData.length; i++) {
        if (subjectData[i][key] === find) {
          subjectData.splice(i, 1);
          return true;
        }
      }
      return false;
    }
    if (this.has(key) && this.get(key) === find) {
      this.delete(key);
      return true;
    }
    return false;
  }

  public isEmpty(): boolean {
    return this.size === 0;
  }

  public isSet(key: string): boolean {
    return this.has(key);
  }

  public getArrayCopy(allow: string[] = [], disallow: string[] = []): Record<string, any> {
    const output: Record<string, any> = {};
    for (const [key, value] of this) {
      if (allow.length && !allow.includes(key)) continue;
      if (disallow.includes(key)) continue;

      output[key] = value instanceof Document ? value.getArrayCopy(allow, disallow) : value;
    }
    return output;
  }

  public clone(): this {
    const clonedDocument = new Document();
    for (const [key, value] of this) {
      clonedDocument.set(key, value instanceof Document ? value.clone() : value);
    }
    return clonedDocument as this;
  }
}