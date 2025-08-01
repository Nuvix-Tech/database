
export interface NuvixEntities {
    [collection: string]: IEntity[];
};

export interface IEntity {
    $id: string;
    $createdAt: Date | string | null;
    $updatedAt: Date | string | null;
    $permissions: string[];
    $sequence: number;
    $collection: string;
}
