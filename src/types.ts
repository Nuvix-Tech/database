import { type Database } from "@core/database.js";
import { Attribute, Index } from "@validators/schema.js";

export interface MetadataEntity {
    $id: string;
    $collection: string;
    name: string;
    attributes: Attribute[];
    indexes: Index[];
    documentSecurity: boolean;
}

export interface Entities {
    [collection: string]: Partial<IEntity>;
    [Database.METADATA]: MetadataEntity;
};

export interface IEntity {
    $id: string;
    $createdAt: Date | string | null;
    $updatedAt: Date | string | null;
    $permissions: string[];
    $sequence: number;
    $collection: string;
    $tenant?: number | null; // Optional tenant ID for multi-tenant support
}
