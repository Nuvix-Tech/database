import { Collection } from "@validators/schema.js";

export interface NuvixDBConfig {
  /** Database collections */
  collections: Collection[];

  /** Type generation options */
  typeGeneration?: {
    /** Output path for generated types */
    outputPath?: string;

    /** Package name for imports */
    packageName?: string;

    /** Include import statements */
    includeImports?: boolean;

    /** Include base IEntity interface */
    includeEntityBase?: boolean;

    /** Include Doc type aliases (e.g., UserDoc = Doc<User>) */
    includeDocTypes?: boolean;

    /** Include entity map interface */
    includeEntityMap?: boolean;

    /** Generate utility types (Create, Update, etc.) */
    generateUtilityTypes?: boolean;

    /** Generate query types for filtering */
    generateQueryTypes?: boolean;

    /** Generate input types for operations */
    generateInputTypes?: boolean;

    /** Generate validation types */
    generateValidationTypes?: boolean;

    includeMetaDataTypes?: boolean;

    /** Custom file header */
    fileHeader?: string;

    /** Additional custom types to append */
    customTypes?: string;
  };

  /** Database connection options */
  database?: {
    host?: string;
    port?: number;
    database?: string;
    username?: string;
    password?: string;
  };

  /** Additional configuration options */
  options?: {
    /** Enable debugging */
    debug?: boolean;

    /** Validation strictness */
    strict?: boolean;

    /** Custom naming conventions */
    naming?: {
      /** Collection naming convention */
      collections?: "camelCase" | "snake_case" | "kebab-case" | "PascalCase";

      /** Attribute naming convention */
      attributes?: "camelCase" | "snake_case" | "kebab-case" | "PascalCase";
    };
  };
}

export interface CLIOptions {
  /** Path to config file */
  config?: string;

  /** Output path override */
  output?: string;

  /** Watch for changes */
  watch?: boolean;

  /** Verbose logging */
  verbose?: boolean;

  /** Dry run (don't write files) */
  dryRun?: boolean;

  /** Force overwrite existing files */
  force?: boolean;
}

export const DEFAULT_CONFIG: Partial<NuvixDBConfig> = {
  typeGeneration: {
    outputPath: "./src/types/generated.ts",
    packageName: "@nuvix-tech/db",
    includeImports: true,
    includeEntityBase: true,
    includeDocTypes: true,
    includeEntityMap: true,
    generateUtilityTypes: true,
    generateQueryTypes: true,
    generateInputTypes: true,
    generateValidationTypes: false,
    fileHeader: `// This file is auto-generated. Do not edit manually.\n// Generated on: ${new Date().toISOString()}\n`,
  },
  options: {
    debug: false,
    strict: true,
    naming: {
      collections: "camelCase",
      attributes: "camelCase",
    },
  },
};
