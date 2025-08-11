import { Doc } from "@core/doc.js";
import { Validator } from "./interface.js";
import { Index } from "./schema.js";

/**
 * A validator to prevent the deletion or renaming of attributes that are
 * part of an existing database index.
 */
export class IndexDependency implements Validator {
  protected message: string =
    "Attribute can't be deleted or renamed because it is used in an index.";

  /**
   * A flag to enable/disable the index dependency validation.
   * If set to false, $valid() always returns true.
   */
  protected enableIndexValidation: boolean;

  /**
   * An array of Document objects, each representing an existing database index.
   */
  protected indexes: Doc<Index>[];

  /**
   * @param indexes - An array of Document objects representing the indexes.
   * @param enableIndexValidation - A flag to toggle this validation on or off. Defaults to true.
   */
  constructor(indexes: Doc<Index>[], enableIndexValidation: boolean = true) {
    this.enableIndexValidation = enableIndexValidation;
    this.indexes = indexes;
  }

  public get $description(): string {
    return this.message;
  }

  /**
   * Checks if the provided attribute Document can be modified without breaking an index.
   *
   * @param value - The attribute Document to validate.
   * @returns {boolean} True if the attribute can be modified, false otherwise.
   */
  public $valid(value: unknown): boolean {
    if (!this.enableIndexValidation) {
      return true;
    }

    if (!(value instanceof Doc)) {
      this.message = "Invalid value provided: Expected a Document instance.";
      return false;
    }

    if (!value.get("array", false)) {
      return true;
    }

    const attributeKey = value.get("key", value.get("$id")).toLowerCase();

    for (const index of this.indexes) {
      const indexedAttributes = index.get("attributes", []) as string[];

      for (const indexedAttributeName of indexedAttributes) {
        if (attributeKey === indexedAttributeName.toLowerCase()) {
          const indexId = index.get("$id", "Unknown Index");
          this.message = `Attribute '${attributeKey}' is used in index '${indexId}' and cannot be modified.`;
          return false;
        }
      }
    }

    return true;
  }
}
