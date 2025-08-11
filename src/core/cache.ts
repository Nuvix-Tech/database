import { Cache as NuvixCache } from "@nuvix/cache";
import { Base } from "./base.js";
import { Collection } from "@validators/schema.js";
import { Doc } from "./doc.js";

export class Cache extends Base {
  protected cacheName: string = "default";

  public getCache(): NuvixCache {
    return this.cache;
  }

  public async purgeCachedCollection(collection: Doc<Collection> | string) {}

  public async purgeCachedDocument(
    collection: string,
    doc: Doc<any> | string,
  ) {}
}
