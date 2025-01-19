import { Logger } from "../core/logger";

export interface Adapter {
  ping(): Promise<void>;

  create(name: string): Promise<boolean>;
}

interface IDatabaseAdapter { }

/**
 * Base adapter class
 */
export abstract class DatabaseAdapter implements IDatabaseAdapter {
  protected options: any;

  protected type: string;

  protected database: string;

  protected schema: string;

  protected sharedTables: boolean = false;

  protected tenantId: number | null = null;

  protected perfix: string;

  /**
   * Debug mode
   */
  protected debug: boolean = true;

  /**
   * Logger instance
   */
  protected logger: Logger;

  /**
   * Transaction counter
   */
  protected inTransaction: number = 0

  constructor() {
    this.type = 'base';
    this.logger = new Logger()
  }

  /**
   * Get the type of the adapter
   */
  public getType(): string {
    return this.type;
  }

  /**
   * Get the database name
   */
  public getDatabase(): string {
    return this.database;
  }

  /**
   * Set the database name
   */
  public setDatabase(database: string): void {
    this.database = database;
    this.options.database = database;
    this.options.connection.database = database;
  }

  /**
   * Get the schema name
   */
  public getSchema(): string {
    return this.schema;
  }

  /**
   * Set the schema name
   */
  public setSchema(schema: string): void {
    this.schema = schema;
  }

  /**
   * Check if shared tables are enabled
   */
  public getSharedTables(): boolean {
    return this.sharedTables;
  }

  /**
   * Set shared tables
   */
  public setSharedTables(sharedTables: boolean): void {
    this.sharedTables = sharedTables;
  }

  /**
   * Get the tenant ID
   */
  public getTenantId(): number | null {
    return this.tenantId;
  }

  /**
   * Set the tenant ID
   */
  public setTenantId(tenantId: number): void {
    this.tenantId = tenantId;
  }

  /**
   * Get the prefix
   */
  public getPrefix(): string {
    return this.perfix;
  }

  /**
   * Set the prefix
   */
  public setPrefix(prefix: string): void {
    this.perfix = this.filter(prefix);
  }

  /**
   * Check if debug mode is enabled
   */
  public getDebug(): boolean {
    return this.debug;
  }

  /**
   * Get the transaction counter
   */
  public getInTransaction(): number {
    return this.inTransaction;
  }

  /**
   * Load module
   */
  protected loadModule(moduleName: string): any {
    return require(moduleName);
  }

  abstract init(): Promise<void>;

  abstract startTransaction(): Promise<boolean>;

  abstract commitTransaction(): Promise<boolean>;

  abstract rollbackTransaction(): Promise<boolean>;

  abstract close(): Promise<void>;

  protected filter(input: string): string {
    return input.replace(/[^\w\s]/gi, '');
  }

  protected trigger<T extends any>(event: any, query: T): T {
    this.logger.debug(`${event}: ${query}`);
    return query;
  }
}