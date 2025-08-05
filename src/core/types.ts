import { Attribute, Index } from "@validators/schema.js";
import type { Database } from "./database.js";
import type { Doc } from "./doc.js";
import { Permission } from "@utils/permission.js";
import { Query } from "./query.js";
import { OnDelete, RelationEnum, RelationSideEnum } from "./enums.js";

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

export type UpdateCollection = {
    id: string;
    permissions: (Permission | string)[];
    documentSecurity: boolean;
}

export type QueryByType = {
    filters: Query[];
    selections: Query[];
    limit: number | null;
    offset: number | null;
    orderAttributes: string[];
    orderTypes: ('ASC' | 'DESC')[];
    cursor: string | number | null;
    cursorDirection: 'AFTER' | 'BEFORE' | null;
    populate: Map<string, QueryByType>;
}


export type CreateRelationshipAttribute = {
    collectionId: string;
    relatedCollectionId: string;
    attribute: string;
    type: RelationEnum;
    relatedAttribute?: string;
    twoWay?: boolean;
    onDelete?: OnDelete;
}
