export enum IndexType {
    KEY = "KEY",
    FULLTEXT = "FULLTEXT",
    UNIQUE = "UNIQUE",
    SPATIAL = "SPATIAL",
}

export interface Index {
    name: string;
    type: IndexType;
    attributes?: string[];
    lengths?: number[];
    orders?: ("ASC" | "DESC")[];
}
