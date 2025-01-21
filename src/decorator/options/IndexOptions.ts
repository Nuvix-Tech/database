export interface IndexOptions {
    $id: string;

    type: "fulltext" | "unique" | "spatial" | "key";

    attributes: string[];

    lengths?: number[] | any[];

    orders?: ("ASC" | "DESC")[];
}
