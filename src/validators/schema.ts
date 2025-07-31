import { AttributeEnum, IndexEnum } from "@core/enums.js";
import { z } from "zod";

export const AttributeType = z.enum(AttributeEnum);

export const AttributeSchema = z.object({
    $id: z.string(),
    key: z.string(),
    type: AttributeType,
    size: z.number().default(0),
    required: z.boolean().default(false),
    signed: z.boolean().default(false),
    array: z.boolean().default(false).optional(),
    filters: z.array(z.string()).optional().default([]),
});

export const IndexType = z.enum(IndexEnum);

export const IndexSchema = z.object({
    $id: z.string(),
    type: IndexType,
    attributes: z.array(z.string()),
    lengths: z.array(z.number()).optional(),
    orders: z.array(z.string()).optional(),
});

export const CollectionSchema = z.object({
    $id: z.string(),
    $collection: z.string(),
    name: z.string(),
    attributes: z.array(AttributeSchema),
    indexes: z.array(IndexSchema).optional(),
    documentSecurity: z.boolean().default(false),
});

export type Collection = z.infer<typeof CollectionSchema>;
export type Attribute = z.infer<typeof AttributeSchema>;
export type Index = z.infer<typeof IndexSchema>;
