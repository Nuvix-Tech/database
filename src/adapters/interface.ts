import { Attribute, Index } from "@validators/schema.js";
import { Meta } from "./base.js";
import { Doc } from "@core/doc.js";
import { ColumnInfo, CreateIndex, UpdateAttribute } from "./types.js";
import { Pool, Client, PoolClient } from 'pg';

export interface IAdapter {

    setMeta(meta: Partial<Meta>): void;
    readonly $database: string;
    readonly $sharedTables: boolean;
    readonly $tenantId?: number;
    readonly $namespace: string;
    readonly $metadata: Record<string, string>;

    quote(name: string): string;
    ping(): Promise<void>;
    create(name: string): Promise<void>;
    exists(name: string): Promise<boolean>;
    delete(name: string): Promise<void>;

    createCollection(options: CreateCollectionOptions): Promise<void>;
    createDocument<D extends Doc>(collection: string, document: D): Promise<D>;
    updateDocument<D extends Doc>(collection: string, document: D, skipPermissions?: boolean): Promise<D>;
    deleteCollection(id: string): Promise<void>;

    getSizeOfCollection(collectionId: string): Promise<number>;
    getSizeOfCollectionOnDisk(collectionId: string): Promise<number>;
    analyzeCollection(collectionId: string): Promise<boolean>;

    deleteIndex(collection: string, id: string): Promise<boolean>;
    createIndex(args: CreateIndex): Promise<boolean>;
    renameIndex(collection: string, oldName: string, newName: string): Promise<boolean>;

    updateAttribute(args: UpdateAttribute): Promise<void>
    getSchemaAttributes(collection: string): Promise<Doc<ColumnInfo>[]>
}

export interface IClient extends Pick<Client, 'query'> {
    $client: Pool | Client | PoolClient;
    $type: 'connection' | 'pool' | 'transaction';
    connect(): Promise<void>;
    disconnect(): Promise<void>;
    transaction<T>(
        callback: () => Promise<T>
    ): Promise<T>;
    ping(): Promise<void>;
    quote(value: string): string;
}

export interface CreateCollectionOptions {
    name: string;
    attributes: Doc<Attribute>[];
    indexes?: Doc<Index>[];
}
