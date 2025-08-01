import { Attribute, Index } from "@validators/schema.js";
import { Meta } from "./base.js";
import { Doc } from "@core/doc.js";

export interface IAdapter {
    readonly $supportsCastIndexArray: boolean;
    readonly $supportsIndex: boolean;
    readonly $supportsUniqueIndex: boolean;
    readonly $supportsFulltextIndex: boolean;
    readonly $supportsFulltextWildcardIndex: boolean;
    readonly $supportsTimeouts: boolean;
    readonly $supportsCasting: boolean;
    readonly $supportsJSONOverlaps: boolean;

    // Limits
    readonly $limitString: number;
    readonly $limitInt: number;
    readonly $limitAttributes: number;
    readonly $limitIndexes: number;

    // Max Sizes
    readonly $maxVarcharLength: number;
    readonly $maxIndexLength: number;

    setMeta(meta: Partial<Meta>): void;
    readonly $database: string;
    readonly $sharedTables: boolean;
    readonly $tenantId?: number;
    readonly $namespace: string;
    readonly $metadata: Record<string, string>;

    ping(): Promise<void>;
    create(name: string): Promise<void>;
    exists(name: string): Promise<boolean>;
    delete(name: string): Promise<void>;

    createCollection(options: CreateCollectionOptions): Promise<void>;

    quote(name: string): string;
}

export interface IClient {
    $client: any;
    $type: 'connection' | 'pool';
    $database: string;
    connect(): Promise<void>;
    disconnect(): Promise<void>;
    transaction<T>(callback: (client: any) => Promise<T>): Promise<T>;
    query<T>(query: string, params?: any[]): Promise<T>;
    ping(): Promise<void>;
}

export interface CreateCollectionOptions {
    name: string;
    attributes: Doc<Attribute>[];
    indexes?: Doc<Index>[];
    documentSecurity?: boolean;
}
