import { AttributeEnum, EventsEnum, PermissionEnum } from "./enums.js";
import { Attribute, Collection } from "@validators/schema.js";
import { Adapter } from "@adapters/base.js";
import { Filters } from "./types.js";
import { Cache } from "./cache.js";
import { Cache as NuvixCache } from '@nuvix/cache';

export class Database extends Cache {
    constructor(adapter: Adapter, cache: NuvixCache, options: DatabaseOptions = {}) {
        super(adapter, cache, options);
    }

    public async create(database?: string): Promise<void> {
        database = database ?? this.adapter.$database;
        await this.adapter.create(database);

        

    }

}


export type DatabaseOptions = {
    tenant?: number;
    filters?: Filters;
};
