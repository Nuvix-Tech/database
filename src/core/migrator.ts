import fs from "fs";
import path from "path";
import { Document } from "./Document";

export class MigrationGenerator {
  static generateMigrationFile(entities: any[], migrationName: string) {
    const upOperations = MigrationGenerator.createUpOperations(entities);
    const downOperations = MigrationGenerator.createDownOperations(
      entities
    );

    const content = `
    import { Document } from "./document";
      
    export default class ${migrationName} {
      static up() {
        return ${MigrationGenerator.stringifyOperations(upOperations)};
      }
      
      static down() {
        return ${MigrationGenerator.stringifyOperations(downOperations)};
      }
    }
    `;

    this.ensureMigrationDirectoryExists()

    const filePath = path.join(__dirname, "migrations", `${migrationName}.js`);
    fs.writeFileSync(filePath, content, "utf8");
    console.log(`Migration ${migrationName} created at ${filePath}`);
  }

  static ensureMigrationDirectoryExists() {
    const migrationsDir = path.join(__dirname, "migrations");
    if (!fs.existsSync(migrationsDir)) {
      fs.mkdirSync(migrationsDir);
    }
  }

  static createUpOperations(entities: any[]) {
    const operations: Document[] = [];

    for (const entity of entities) {

      operations.push(
        entity instanceof Document ? entity : new Document(entity)
      );
    }

    return operations;
  }

  static createDownOperations(entities: any[]) {
    const operations: Document[] = [];

    for (const entity of entities) {
      operations.push(
        new Document({
          $id: entity.getId(),
        })
      );
    }

    return operations;
  }

  static stringifyOperations(operations: Document[]) {
    return `[${operations
      .map((operation) => `new Document(${MigrationGenerator.stringifyDocument(operation)})`)
      .join(",\n")}]`;
  }

  static stringifyDocument(document: Document) {
    const obj: any = {};
    document.forEach((value, key) => {
      if (value instanceof Document) {
        obj[key] = MigrationGenerator.stringifyDocument(value);
      } else if (Array.isArray(value)) {
        obj[key] = value.map((item) =>
          item instanceof Document ? MigrationGenerator.stringifyDocument(item) : item
        );
      } else {
        obj[key] = value;
      }
    });
    return JSON.stringify(obj);
  }
}
