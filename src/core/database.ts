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

}


export type DatabaseOptions = {
    tenant?: number;
    filters?: Filters;
};
