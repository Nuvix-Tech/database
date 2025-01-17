export interface Adapter {
  ping(): Promise<void>;
}

interface IDatabaseAdapter { }

/**
 * Base adapter class
 */
export abstract class DatabaseAdapter implements IDatabaseAdapter {
  type: string;
  database: string;

  /**
   * Transaction counter
   */
  inTransaction: number = 0

  constructor() {
    this.type = 'base';
  }

  loadModule(moduleName: string): any {
    return require(moduleName);
  }

  abstract init(): Promise<void>;

  abstract startTransaction(): Promise<boolean>;

  abstract commitTransaction(): Promise<boolean>;

  abstract rollbackTransaction(): Promise<boolean>;

  abstract close(): Promise<void>;

}