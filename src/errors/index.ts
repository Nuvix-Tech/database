export { default as AuthorizationException } from "./authorization.js";
export { default as ConflictException } from "./conflict.js";
export { default as DependencyException } from "./dependency.js";
export { default as DuplicateException } from "./duplicate.js";
export { default as LimitException } from "./limit.js";
export { default as OrderException } from "./order.js";
export { default as NotFoundException } from "./not-found.js";
export { default as QueryException } from "./query.js";
export { default as RelationshipException } from "./relationship.js";
export { default as RestrictedException } from "./restricted.js";
export { default as StructureException } from "./structure.js";
export { default as TimeoutException } from "./timeout.js";
export { default as TransactionException } from "./transaction.js";
export { default as TruncateException } from "./truncate.js";
import { DatabaseException } from "./base.js";

export class IndexException extends DatabaseException {
  constructor(message: string) {
    super(message);
    this.name = "IndexException";
  }
}

export { DatabaseException };
