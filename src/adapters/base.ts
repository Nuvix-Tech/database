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
}

interface Meta {
    database: string;
    schema: string;
    sharedTables: boolean;
    tenantId: number;
    namespace: string;
    metadata: Record<string, string>
}
