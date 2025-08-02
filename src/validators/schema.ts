import { AttributeEnum, IndexEnum } from "@core/enums.js";
import { z } from "zod";

export const AttributeType = z.enum(AttributeEnum);

export const AttributeSchema = z.object({
    $id: z.string(),
    key: z.string().optional(),
    type: AttributeType,
    size: z.number().default(0).optional(),
    required: z.boolean().default(false).optional(),
    signed: z.boolean().default(true).optional(),
    array: z.boolean().default(false).optional(),
    filters: z.array(z.string()).default([]).optional(),
    default: z.any().optional(),
    options: z.record(z.string(), z.any()).optional(),
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
    documentSecurity: z.boolean().default(false).optional(),
});

export type Collection = z.infer<typeof CollectionSchema>;
export type Attribute = z.infer<typeof AttributeSchema>;
export type Index = z.infer<typeof IndexSchema>;
