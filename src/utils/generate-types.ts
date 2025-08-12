import { AttributeEnum } from "@core/enums.js";
import { Collection, RelationOptions } from "@validators/schema.js";

const typeMap: Record<AttributeEnum, string> = {
  [AttributeEnum.String]: "string",
  [AttributeEnum.Integer]: "number",
  [AttributeEnum.Float]: "number",
  [AttributeEnum.Boolean]: "boolean",
  [AttributeEnum.Timestamptz]: "Date | string",
  [AttributeEnum.Json]: "Record<string, any>",
  [AttributeEnum.Relationship]: "string", // will be replaced dynamically
  [AttributeEnum.Virtual]: "never",
  [AttributeEnum.Uuid]: "string",
};

export function generateTypes(collections: Collection[]): string {
  const IEntityBase = `
export interface IEntity {
  $id: string;
  $createdAt: Date | string | null;
  $updatedAt: Date | string | null;
  $permissions: string[];
  $sequence: number;
  $collection: string;
  $tenant?: number | null;
}
    `;

  const entityInterfaces = collections.map((col) => {
    const attrs = col.attributes
      .map((attr) => {
        let tsType: string;

        // Relationship type handling
        if (attr.type === AttributeEnum.Relationship) {
          const opts = attr.options as RelationOptions;
          const related = collections.find(
            (c) => c.$id === opts.relatedCollection,
          );
          if (related) {
            tsType = pascalCase(related.name) + "['$id']";
          } else {
            tsType = "string";
          }
        }
        // Enum literal handling
        else if (attr.format === "enum" && attr.formatOptions?.["values"]) {
          const values = attr.formatOptions["values"]
            .map((v: string) => JSON.stringify(v))
            .join(" | ");
          tsType = values;
        }
        // Default type mapping
        else {
          tsType = typeMap[attr.type as AttributeEnum] ?? "any";
        }

        if (attr.array) tsType += "[]";
        const optional = attr.required ? "" : "?";
        return `    ${attr.key}${optional}: ${tsType};`;
      })
      .join("\n");

    return `export interface ${pascalCase(col.name)} extends IEntity {\n${attrs}\n}`;
  });

  const entityMap = `
export interface Entities {
${collections
      .map((col) => {
        const interfaceName = pascalCase(col.name);
        return ` "${col.$id}": ${interfaceName};`;
      })
      .join("\n")}
}`;
  const result = [IEntityBase, ...entityInterfaces, entityMap].join("\n\n");
  console.log(result)
  return result;
}

function pascalCase(str: string) {
  return str
    .split(/[^a-zA-Z0-9]/)
    .filter(Boolean)
    .map((word) => word.charAt(0).toUpperCase() + word.slice(1))
    .join("");
}
