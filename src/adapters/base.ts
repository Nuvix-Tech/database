import { DatabaseException } from "@errors/base.js";
import { EventEmitter } from "stream";
import { IClient } from "./interface.js";

export abstract class BaseAdapter extends EventEmitter {
    public readonly type: string = 'base';
    protected _meta: Partial<Meta> = {};
    protected abstract client: IClient;

    public get $database(): string {
        if (!this._meta.database) throw new DatabaseException('Database name is not defined in adapter metadata.');
        return this._meta.database;
    }

    public get $sharedTables(): boolean {
        return !!this._meta.sharedTables;
    }

    public get $tenantId(): number | undefined {
        return this._meta.tenantId;
    }

    public get $namespace(): string {
        return this._meta.namespace ?? 'default';
    }

    public get $metadata() {
        return this._meta.metadata ?? {};
    }

    public setMeta(meta: Partial<Meta>) {
        this._meta = meta;
    }

    protected sanitize(value: string): string {
        if (value === null || value === undefined) {
            throw new DatabaseException(
                "Failed to sanitize key: value is null or undefined",
            );
        }

        const sanitized = value.replace(/[^A-Za-z0-9_\-]/g, "");
        if (sanitized === "") {
            throw new DatabaseException(
                "Failed to sanitize key: filtered value is empty",
            );
        }

        return sanitized;
    }
}

export interface Meta {
    database: string;
    schema: string;
    sharedTables: boolean;
    tenantId: number;
    namespace: string;
    metadata: Record<string, string>
}
