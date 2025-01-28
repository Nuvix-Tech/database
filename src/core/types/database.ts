import { Document } from "../Document";
import { Query } from "../query";

export interface CreateCollectionParams {
    id: string;
    attributes?: Document[];
    indexes?: Document[];
    permissions?: string[] | null;
    documentSecurity?: boolean;
}

export interface UpdateCollectionParams {
    id: string;
    permissions: string[];
    documentSecurity: boolean;
}

export interface CreateAttributeParams {
    collection: string;
    id: string;
    type: string;
    size: number;
    required: boolean;
    defaultValue?: any;
    signed?: boolean;
    array?: boolean;
    format?: string | null;
    formatOptions?: Record<string, any>;
    filters?: string[];
}

export interface UpdateAttributeParams {
    collection: string;
    id: string;
    type?: string | null;
    size?: number | null;
    required?: boolean | null;
    defaultValue?: any;
    signed?: boolean | null;
    array?: boolean | null;
    format?: string | null;
    formatOptions?: Record<string, any> | null;
    filters?: string[] | null;
    newKey?: string | null;
}

export interface CreateRelationshipParams {
    collection: string;
    relatedCollection: string;
    type: string;
    twoWay?: boolean;
    id?: string | null;
    twoWayKey?: string | null;
    onDelete?: string;
}

export interface UpdateRelationshipParams {
    collection: string;
    id: string;
    newKey?: string | null;
    newTwoWayKey?: string | null;
    twoWay?: boolean | null;
    onDelete?: string | null;
}

export interface CreateIndexParams {
    collection: string;
    id: string;
    type: string;
    attributes: string[];
    lengths?: number[];
    orders?: string[];
}

export interface GetDocumentParams {
    collection: string;
    id: string;
    queries?: Query[];
    forUpdate?: boolean;
}

export interface IncreaseDocumentAttributeParams {
    collection: string;
    id: string;
    attribute: string;
    value?: number;
    max?: number | null;
}

export interface DecreaseDocumentAttributeParams {
    collection: string;
    id: string;
    attribute: string;
    value?: number;
    min?: number | null;
}

export interface FindParams {
    collection: string;
    queries?: Query[];
    forPermission?: string;
}
