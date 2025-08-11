import { Attribute, Index } from "@validators/schema.js";
import { Doc } from "@core/doc.js";
import { Pool, Client, PoolClient } from "pg";

export interface IClient extends Pick<Client, "query"> {
  $client: Pool | Client | PoolClient;
  $type: "connection" | "pool" | "transaction";
  connect(): Promise<void>;
  disconnect(): Promise<void>;
  transaction<T>(callback: () => Promise<T>): Promise<T>;
  ping(): Promise<void>;
  quote(value: string): string;
}

export interface CreateCollectionOptions {
  name: string;
  attributes: Doc<Attribute>[];
  indexes?: Doc<Index>[];
}
