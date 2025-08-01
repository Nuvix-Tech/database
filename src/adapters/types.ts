import { AttributeEnum, IndexEnum } from "@core/enums.js";
import { Attribute } from "@validators/schema.js";

export type CreateAttribute = {
    collection: string;
    name: string;
    type: AttributeEnum;
    size: number;
    signed?: boolean;
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
