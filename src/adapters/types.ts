import { AttributeEnum } from "@core/enums.js";

export type CreateAttribute = {
    collection: string;
    name: string;
    type: AttributeEnum;
    size: number;
    signed?: boolean;
    array?: boolean;
}
