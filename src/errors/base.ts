export class DatabaseError extends Error {
  constructor(
    message: string,
    public code?: string,
    error?: any,
  ) {
    super(message);
    this.name = this.constructor.name;
  }
}

export { DatabaseError as DatabaseException };
