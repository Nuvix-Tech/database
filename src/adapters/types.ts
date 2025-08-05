import { AttributeEnum, CursorEnum, IndexEnum, OrderEnum, PermissionEnum, RelationEnum, RelationSideEnum } from "@core/enums.js";
import { Query } from "@core/query.js";
import { QueryBuilder } from "@utils/query-builder.js";
import { Attribute } from "@validators/schema.js";

export type CreateAttribute = {
    collection: string;
    key: string;
    type: AttributeEnum;
    size?: number;
    array?: boolean;
}

export type UpdateAttribute = CreateAttribute & {
    newName?: string
}

export type CreateIndex = {
    collection: string;
    name: string;
    type: IndexEnum;
    attributes: string[];
    lengths?: number[];
    orders?: string[];
    attributeTypes: AttributeEnum[];
}

export interface ColumnInfo {
    $id: string;
    columnDefault: string | null;
    isNullable: "YES" | "NO";
    dataType: string;
    characterMaximumLength: number | null;
    numericPrecision: number | null;
    numericScale: number | null;
    datetimePrecision: number | null;
    columnType: string;
    columnKey: string;
    extra: string;
}

export interface IncreaseDocumentAttribute {
    collection: string;
    id: string;
    attribute: string;
    value: number;
    updatedAt: Date;
    min?: number;
    max?: number;
}

export interface Find {
    collection: string;
    query?: ((builder: QueryBuilder) => QueryBuilder) | Query[];
    options?: {
        limit?: number;
        offset?: number;
        orderAttributes?: string[];
        orderTypes?: OrderEnum[];
        cursor?: Record<string, string | number>
        cursorDirection?: CursorEnum
        permission?: PermissionEnum
    };
}

export interface CreateRelationship {
    collection: string;
    attribute: string;
    type: RelationEnum;
    twoWay?: boolean;
    target: {
        collection: string;
        attribute?: string;
    },
    junctionCollection?: string;
    onDelete?: {
        action: "cascade" | "set null" | "restrict";
        side: RelationSideEnum;
    };
}
