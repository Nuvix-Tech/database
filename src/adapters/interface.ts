import { Attribute, Index } from "@validators/schema.js";
import { Meta } from "./base.js";
import { Doc } from "@core/doc.js";

export interface IAdapter {
    readonly $limitForString: number;
    readonly $limitForInt: number;
    readonly $limitForAttributes: number;
    readonly $limitForIndexes: number;
    readonly $supportForSchemas: boolean;
    readonly $supportForIndex: boolean;
    readonly $supportForAttributes: boolean;
    readonly $supportForUniqueIndex: boolean;
    readonly $supportForFulltextIndex: boolean;
    readonly $supportForUpdateLock: boolean;
    readonly $supportForAttributeResizing: boolean;
    readonly $supportForBatchOperations: boolean;
    readonly $supportForGetConnectionId: boolean;
    readonly $supportForCacheSkipOnFailure: boolean;
    readonly $supportForHostname: boolean;
    readonly $documentSizeLimit: number;
    readonly $supportForCasting: boolean;
    readonly $supportForNumericCasting: boolean;
    readonly $supportForQueryContains: boolean;
    readonly $supportForIndexArray: boolean;
    readonly $supportForCastIndexArray: boolean;
    readonly $supportForRelationships: boolean;
    readonly $supportForReconnection: boolean;
    readonly $supportForBatchCreateAttributes: boolean;

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
    createDocument<D extends Doc>(collection: string, document: D): Promise<D>;
    updateDocument<D extends Doc>(collection: string, document: D, skipPermissions?: boolean): Promise<D>;

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
    quote(name: string): string;
}

export interface CreateCollectionOptions {
    name: string;
    attributes: Doc<Attribute>[];
    indexes?: Doc<Index>[];
}
