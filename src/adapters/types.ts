import { AttributeEnum } from "@core/enums.js";

export type CreateAttribute = {
    collection: string;
    name: string;
    type: AttributeEnum;
    size: number;
    signed?: boolean;
    array?: boolean;
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
