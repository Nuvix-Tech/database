import { InitializeError } from "../errors/adapter";
import { DatabaseError } from "../errors/base";
import { Adapter, DatabaseAdapter } from "./base";
import * as mysql2 from 'mysql2/promise';

interface MariaDBOptions {
  host: string;
  port: number;
  user: string;
  password: string;
}

/**
 * MariaDB adapter class
 */
export class MariaDB extends DatabaseAdapter implements Adapter {
  /**
   * MariaDB pool / connection
   */
  pool: mysql2.Pool;
  library: typeof mysql2;

  /**
   * MariaDB adapter instance
   */
  instance: this;

  /**
   * MariaDB adapter options
   */
  options: MariaDBOptions;

  constructor(options: MariaDBOptions) {
    super()
    this.library = this.loadModule('mysql2/promise');
    this.type = 'mariadb';
    this.options = options;
  }

  /**
   * Initialize MariaDB adapter
   * @throws {InitializeError} If adapter is already initialized
   */
  async init() {
    if (this.instance) throw new InitializeError('MariaDB adapter already initialized');

    try {
      let pool = this.library.createPool({
        host: this.options.host,
        port: this.options.port,
        user: this.options.user,
        password: this.options.password,
        rowsAsArray: true,
        debug: true
      });

      await pool.ping();

      this.pool = pool;
      this.instance = this;
    } catch (e) {
      throw new InitializeError('MariaDB adapter initialization failed');
    }
  }

  /**
   * Ping MariaDB adapter
   */
  async ping() {
    try {
      await this.pool.ping()
    } catch (e) {
      throw new InitializeError('MariaDB adapter ping failed');
    }
  }


  /**
   * Start MariaDB transaction
   */
  async startTransaction(): Promise<boolean> {
    try {
      if (this.inTransaction === 0) {
        await this.pool.query('START TRANSACTION');
      }

      this.inTransaction++;
      return true;
    } catch (e) {
      throw new DatabaseError('MariaDB adapter start transaction failed');
    }
  }

  /**
   * Commit MariaDB transaction
   */
  async commitTransaction() {
    try {
      if (this.inTransaction === 1) {
        await this.pool.query('COMMIT');
      }
      this.inTransaction--;
      return true
    } catch (e) {
      throw new DatabaseError('MariaDB adapter commit transaction failed');
    }
  }

  /**
   * Rollback MariaDB transaction
   */
  async rollbackTransaction() {
    try {
      if (this.inTransaction === 1) {
        await this.pool.query('ROLLBACK');
      }
      this.inTransaction--;
      return true
    } catch (e) {
      throw new DatabaseError('MariaDB adapter rollback transaction failed');
    }
  }

  /**
   * Close MariaDB connection
   */
  async close() {
    try {
      await this.pool.end();
    } catch (e) {
      throw new DatabaseError('MariaDB adapter close failed');
    }
  }

  /**
   * Create Database
   */

}