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
  protected type: string;

  protected database: string;

  protected schema: string;

  protected sharedTables: boolean = false;

  protected tenantId: string;

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

  filter(input: string): string {
    return input.replace(/[^\w\s]/gi, '');
  }
}