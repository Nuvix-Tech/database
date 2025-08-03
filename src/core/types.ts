import { Attribute, Index } from "@validators/schema.js";
import type { Database } from "./database.js";
import type { Doc } from "./doc.js";
import { Permission } from "@utils/permission.js";

export type FilterValue = string | number | boolean | Date | null | undefined | object | any[] | Record<string, any> | FilterValue[];

export type Filter<T = FilterValue, U = FilterValue, D = Doc> = {
    encode: (value: T, document: D, db: Database) => U | Promise<U>;
    decode: (value: U, document: D, db: Database) => T | Promise<T>;
};

export type Filters = Record<string, Filter>;

export type CreateCollection = {
    id: string;
    attributes?: Doc<Attribute>[];
    indexes?: Doc<Index>[];
    permissions?: (Permission | string)[];
    documentSecurity?: boolean;
}
