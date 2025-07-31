
export interface NuvixEntities {

};

export interface IEntity {
    $id: string;
    $createdAt: Date | string | null;
    $updatedAt: Date | string | null;
    $permissions: string[];
    $sequence: number;
    $collection: string;
}
