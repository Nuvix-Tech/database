import { Doc } from "@nuvix-tech/db";


export interface IEntity {
  $id: string;
  $createdAt: Date | string | null;
  $updatedAt: Date | string | null;
  $permissions: string[];
  $sequence: number;
  $collection: string;
  $tenant?: number | null;
}

export interface TestCollection extends IEntity {
    required_field: string;
    /** @optional */
    optional_field?: string;
}

// Document Types
export type TestCollectionDoc = Doc<TestCollection>;

// Utility Types

// Utility types for TestCollection
export type TestCollectionCreate = Omit<TestCollection, '$id' | '$createdAt' | '$updatedAt' | '$sequence'>;
export type TestCollectionUpdate = Partial<TestCollectionCreate>;
export type TestCollectionKeys = keyof TestCollection;
export type TestCollectionValues = TestCollection[TestCollectionKeys];
export type TestCollectionPick<K extends keyof TestCollection> = Pick<TestCollection, K>;
export type TestCollectionOmit<K extends keyof TestCollection> = Omit<TestCollection, K>;

// Query Types

// Query types for TestCollection
export type TestCollectionQuery = {
  [K in keyof TestCollection]?: TestCollection[K] | { $in?: TestCollection[K][] } | { $ne?: TestCollection[K] } | { $exists?: boolean } | { $gt?: TestCollection[K] } | { $gte?: TestCollection[K] } | { $lt?: TestCollection[K] } | { $lte?: TestCollection[K] } | { $regex?: string } | { $contains?: string };
} & {
  $or?: TestCollectionQuery[];
  $and?: TestCollectionQuery[];
  $limit?: number;
  $offset?: number;
  $orderBy?: { [K in keyof TestCollection]?: 'asc' | 'desc' };
};

// Input Types

// Input types for TestCollection
export type TestCollectionInput = Omit<TestCollection, '$id' | '$createdAt' | '$updatedAt' | '$permissions' | '$sequence' | '$collection' | '$tenant'>;
export type TestCollectionCreateInput = TestCollectionInput;
export type TestCollectionUpdateInput = Partial<TestCollectionInput>;

export interface Entities {
  "test": TestCollection;
}

// Collection Metadata

// Metadata for TestCollection
export const TestCollectionMetadata = {
  $id: "test",
  name: "test_collection",
  collectionName: "test_collection",
  attributes: [
  {
    "key": "required_field",
    "type": "string",
    "required": true,
    "array": false,
    "format": null
  },
  {
    "key": "optional_field",
    "type": "string",
    "required": false,
    "array": false,
    "format": null
  }
],
  indexes: [],
  documentSecurity: false
} as const;

// All Collections Metadata
export const AllCollectionsMetadata = {
  "test": TestCollectionMetadata
} as const;

export type CollectionId = keyof typeof AllCollectionsMetadata;