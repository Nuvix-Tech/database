import { Meta } from "./base.js";


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
}

export interface IClient {
    connect(): Promise<void>;
    disconnect(): Promise<void>;
    transaction<T>(callback: (client: any) => Promise<T>): Promise<T>;
}
