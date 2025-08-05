import { AttributeEnum, IndexEnum, OnDelete, RelationEnum, RelationSideEnum } from "@core/enums.js";
import { z } from "zod";

export const AttributeType = z.enum(AttributeEnum);

const AttributeOptions = z.object({
    type: z.enum(RelationEnum),
    side: z.enum(RelationSideEnum),
    relatedCollection: z.string(),
    twoWay: z.boolean().default(false).optional(),
    relatedAttribute: z.string().optional(),
    onDelete: z.enum(OnDelete),
});

export const AttributeSchema = z.object({
    $id: z.string(),
    key: z.string(),
    type: AttributeType,
    size: z.number().default(0).optional(),
    required: z.boolean().default(false).optional(),
    array: z.boolean().default(false).optional(),
    filters: z.array(z.string()).default([]).optional(),
    format: z.string().optional(),
    formatOptions: z.record(z.string(), z.any()).optional(),
    default: z.any().optional(),
    options: z.union([AttributeOptions, z.record(z.string(), z.any())]).optional(),
});

export const IndexType = z.enum(IndexEnum);

export const IndexSchema = z.object({
    $id: z.string(),
    type: IndexType,
    attributes: z.array(z.string()).optional(),
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
