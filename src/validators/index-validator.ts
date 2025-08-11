import { Doc } from "@core/doc.js";
import { Validator } from "./interface.js";
import { Attribute } from "./schema.js";
import { Database } from "@core/database.js";
import { type Index as IndexType } from "./schema.js";
import { AttributeEnum, IndexEnum } from "@core/enums.js";

/**
 * Helper function to capitalize the first letter of a string.
 */
function capitalize(str: string): string {
  if (!str) return "";
  return str.charAt(0).toUpperCase() + str.slice(1);
}

/**
 * Validates a database index definition.
 * It checks for attribute existence, duplicates, type compatibility for fulltext indexes,
 * array attribute rules, index length limits, and reserved names.
 */
export class Index implements Validator {
  protected message: string = "Invalid index.";
  protected maxLength: number;
  protected attributes: Record<string, Doc<Attribute>> = {};
  protected reservedKeys: string[];

  /**
   * Index constructor.
   *
   * @param attributes - An array of `Doc<AttributeType>` objects representing the available attributes in the schema.
   * @param maxLength - The maximum allowed combined length for an index. 0 for unlimited.
   * @param reservedKeys - An array of string keys that are reserved and cannot be used as index names.
   * @throws {Error} If `maxLength` is negative.
   */
  constructor(
    attributes: Doc<Attribute>[],
    maxLength: number,
    reservedKeys: string[] = [],
    private readonly arrayIndexSupport: boolean = false,
  ) {
    if (maxLength < 0) {
      throw new Error("Index maximum length must be a non-negative number.");
    }
    this.maxLength = maxLength;
    this.reservedKeys = reservedKeys.map((key) => key.toLowerCase());

    for (const attribute of attributes) {
      const key = attribute.get("key", attribute.get("$id", "")).toLowerCase();
      if (key) {
        this.attributes[key] = attribute;
      }
    }

    for (const attributeData of Database.INTERNAL_ATTRIBUTES) {
      const key = (attributeData["$id"] ?? "").toLowerCase();
      if (key) {
        this.attributes[key] = new Doc<Attribute>(attributeData as Attribute);
      }
    }
  }

  /**
   * Returns the validator's description (error message).
   * @returns {string}
   */
  public get $description(): string {
    return this.message;
  }

  /**
   * Validates if an index definition is valid.
   * This method orchestrates all specific validation checks.
   *
   * @param value - The `Doc<IndexType>` object representing the index to validate.
   * @returns {boolean} True if the index is valid, false otherwise.
   */
  public $valid(value: Doc<IndexType>): boolean {
    this.message = "Invalid index.";

    if (!this.checkAttributesNotFound(value)) return false;
    if (!this.checkEmptyIndexAttributes(value)) return false;
    if (!this.checkDuplicatedAttributes(value)) return false;
    if (!this.checkFulltextIndexNonString(value)) return false;
    if (!this.checkArrayIndex(value)) return false;
    if (!this.checkReservedNames(value)) return false;

    return true;
  }

  /**
   * Checks if all attributes specified in the index exist in the schema.
   * @param index - The index `Doc` to validate.
   * @returns {boolean} True if all index attributes are found, false otherwise.
   */
  protected checkAttributesNotFound(index: Doc<IndexType>): boolean {
    const indexAttributes = index.get("attributes", []);
    for (const attributeName of indexAttributes) {
      if (!this.attributes.hasOwnProperty(attributeName.toLowerCase())) {
        this.message = `Invalid index attribute "${attributeName}" not found in schema.`;
        return false;
      }
    }
    return true;
  }

  /**
   * Checks if the index has at least one attribute defined.
   * @param index - The index `Doc` to validate.
   * @returns {boolean} True if index attributes are not empty, false otherwise.
   */
  protected checkEmptyIndexAttributes(index: Doc<IndexType>): boolean {
    if (index.get("attributes", []).length === 0) {
      this.message = "No attributes provided for index.";
      return false;
    }
    return true;
  }

  /**
   * Checks for duplicated attributes within the index definition.
   * @param index - The index `Doc` to validate.
   * @returns {boolean} True if no duplicated attributes are found, false otherwise.
   */
  protected checkDuplicatedAttributes(index: Doc<IndexType>): boolean {
    const attributes = index.get("attributes", []);
    const seenAttributes = new Set<string>();

    for (const attributeName of attributes) {
      const lowercasedAttribute = attributeName.toLowerCase();
      if (seenAttributes.has(lowercasedAttribute)) {
        this.message = `Duplicate attribute "${attributeName}" provided in index.`;
        return false;
      }
      seenAttributes.add(lowercasedAttribute);
    }
    return true;
  }

  /**
   * Checks if a fulltext index contains only string attributes.
   * @param index - The index `Doc` to validate.
   * @returns {boolean} True if fulltext index attributes are valid, false otherwise.
   */
  protected checkFulltextIndexNonString(index: Doc<IndexType>): boolean {
    if (index.get("type") === IndexEnum.FullText) {
      const indexAttributes = index.get("attributes", []);
      for (const attributeName of indexAttributes) {
        const attrDoc = this.attributes[attributeName.toLowerCase()]!;
        if (attrDoc.get("type", "") !== AttributeEnum.String) {
          const attrKey = attrDoc.get("key", attrDoc.get("$id", ""));
          this.message = `Attribute "${attrKey}" cannot be part of a FULLTEXT index; it must be of type "${AttributeEnum.String}".`;
          return false;
        }
      }
    }
    return true;
  }

  /**
   * Checks rules specific to array attributes within an index.
   * An index may only contain one array attribute, and specific index types/orders are forbidden.
   * @param index - The index `Doc` to validate.
   * @returns {boolean} True if array index rules are followed, false otherwise.
   */
  protected checkArrayIndex(index: Doc<IndexType>): boolean {
    const attributes = index.get("attributes", []);
    const orders = index.get("orders", []);

    const arrayAttributesInIndex: string[] = [];

    for (let i = 0; i < attributes.length; i++) {
      const attributeName = attributes[i]!;
      const attrDoc = this.attributes[attributeName.toLowerCase()]!;

      if (attrDoc.get("array", false) === true) {
        if (index.get("type") !== IndexEnum.Key) {
          this.message = `"${capitalize(index.get("type"))}" index is forbidden on array attributes.`;
          return false;
        }

        arrayAttributesInIndex.push(attrDoc.get("key", ""));

        if (arrayAttributesInIndex.length > 1) {
          this.message = "An index may only contain one array attribute.";
          return false;
        }

        const direction = orders[i];
        if (direction) {
          this.message = `Invalid index order "${direction}" on array attribute "${attrDoc.get("key", attrDoc.get("$id", ""))}".`;
          return false;
        }

        if (this.arrayIndexSupport === false) {
          this.message = "Indexing an array attribute is not supported";
          return false;
        }
      }
    }
    return true;
  }

  /**
   * Checks if the index's key name is among the reserved keys.
   * @param index - The index `Doc` to validate.
   * @returns {boolean} True if the index key is not reserved, false otherwise.
   */
  protected checkReservedNames(index: Doc<IndexType>): boolean {
    const indexKey = index.get("key", index.get("$id", "")).toLowerCase();

    if (this.reservedKeys.includes(indexKey)) {
      this.message = `Index name "${indexKey}" is reserved.`;
      return false;
    }
    return true;
  }
}
