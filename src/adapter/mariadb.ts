import { Database } from "../core/database";
import { Attribute } from "../core/types/attribute";
import { Index, IndexType } from "../core/types/indexes";
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
      const pool = this.library.createPool({
        host: this.options.host,
        port: this.options.port,
        user: this.options.user,
        password: this.options.password,
        rowsAsArray: true,
      });

      const connection = await pool.getConnection();
      await connection.ping();
      connection.release();

      this.pool = pool;
      this.instance = this;
    } catch (e) {
      this.logger.error(e)
      throw new InitializeError('MariaDB adapter initialization failed');
    }
  }

  /**
   * Ping MariaDB adapter
   */
  async ping() {
    try {
      const connection = await this.pool.getConnection();
      await connection.ping();
      connection.release();
      this.logger.debug('PING SUCCESS')
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
   * Set Database
   */
  async use(name: string): Promise<boolean> {
    try {
      const [result] = await this.pool.query<any>(`USE ${name}`);
      return result.affectedRows > 0;
    } catch (e) {
      this.logger.error(e)
      throw new DatabaseError("Database Selection Failed.")
    }
  }

  /**
   *  Get Tenant SQL
   */
  private getTenantSql(tenantId?: string) {
    tenantId = tenantId ? tenantId : this.tenantId;

    if (this.sharedTables && !this.tenantId) throw new DatabaseError('Tenant ID is required for shared tables')

    if (this.tenantId && this.sharedTables) {
      return ` AND tenant_id = '${tenantId}'`
    }
    return '';
  }

  /**
   * Create Database
   */
  async create(name: string): Promise<boolean> {
    try {
      const [result] = await this.pool.query<any>(`CREATE SCHEMA \`hello\``);
      return result.affectedRows > 0;
    } catch (e) {
      this.logger.error(e)
      throw new DatabaseError("Database Creation Failed.")
    }
  }


  /**
   * Drops a schema from the database if it exists.
   *
   * @param name - The name of the schema to drop.
   * @returns A promise that resolves to a boolean indicating whether the schema was successfully dropped.
   * @throws {DatabaseError} If the schema drop operation fails.
   */
  async drop(name: string): Promise<boolean> {
    try {
      const [result] = await this.pool.query<any>(`DROP SCHEMA IF EXISTS \`hello\``);
      return result.affectedRows > 0;
    } catch (e) {
      this.logger.error(e)
      throw new DatabaseError("Database Drop Failed.")
    }
  }


  /**
   * Creates a new collection in the MariaDB database with the specified name, attributes, and indexes.
   * 
   * @param name - The name of the collection to be created.
   * @param attributes - An array of attributes defining the structure of the collection.
   * @param indexes - An array of indexes to be applied to the collection.
   * @param ifExists - A boolean flag indicating whether to include the "IF NOT EXISTS" clause in the SQL statement. Defaults to false.
   * @returns A promise that resolves to a boolean indicating whether the collection was successfully created.
   * @throws {DatabaseError} Throws an error if the table creation fails.
   */
  async createCollection(name: string, attributes: Attribute[], indexes: Index[], ifExists: boolean = false): Promise<boolean> {
    name = this.filter(name)

    if (this.perfix) name = this.perfix + name;

    let attributeSql = attributes.map((attribute) => {
      return `${attribute.name} ${this.getSqlType(attribute.type, attribute.size, attribute.signed, attribute.array)}`
    }).join(',');

    let indexSql = indexes.map((index) => {
      let indexAttributes = index.attributes?.map((attribute, nested) => {
        let indexLength = index.lengths?.[nested] ?? '';
        indexLength = (indexLength) ? `(${indexLength})` : '';
        let indexOrder = index.orders?.[nested] ?? '';

        let indexAttribute = attribute;
        if (attribute === '$id') indexAttribute = '_uid';
        if (attribute === '$createdAt') indexAttribute = '_createdAt';
        if (attribute === '$updatedAt') indexAttribute = '_updatedAt';

        if (index.type === IndexType.FULLTEXT) indexOrder = '';

        return `\`${indexAttribute}\`${indexLength} ${indexOrder}`;
      }).join(', ');

      if (this.sharedTables && index.type !== IndexType.FULLTEXT) {
        indexAttributes = `_tenant ${indexAttributes ? `, ${indexAttributes}` : ''}`;
      }

      return `${index.type} \`${index.name}\` ${indexAttributes ? `(${indexAttributes})` : ''}`;
    }).join(',');

    ifExists = ifExists ? 'IF NOT EXISTS' : '' as any;

    let collectionSql = `
      CREATE TABLE ${ifExists} ${name} (
      _id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT,
      _uid VARCHAR(255) NOT NULL,
      _createdAt DATETIME(3) DEFAULT NULL,
      _updatedAt DATETIME(3) DEFAULT NULL,
      PRIMARY KEY (_id),
      ${attributeSql}${attributeSql ? ',' : ''} 
      ${indexSql}${indexSql ? ',' : ''}
    `;

    if (this.sharedTables) {
      collectionSql += `
      _tenant INT(11) UNSIGNED DEFAULT NULL,
      UNIQUE KEY _uid (_uid, _tenant),
      KEY _created_at (_tenant, _createdAt),
      KEY _updated_at (_tenant, _updatedAt),
      KEY _tenant_id (_tenant, _id)
      `;
    } else {
      collectionSql += `
      UNIQUE KEY _uid (_uid),
      KEY _created_at (_createdAt),
      KEY _updated_at (_updatedAt)
      `;
    }

    collectionSql += `)`;

    let permissionsSql = `
      CREATE TABLE ${ifExists} ${name}_perms (
        _id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT,
        _type VARCHAR(12) NOT NULL,
        _permission VARCHAR(255) NOT NULL,
        _document VARCHAR(255) NOT NULL,
        PRIMARY KEY (_id),
    `;

    if (this.sharedTables) {
      permissionsSql += `
        _tenant INT(11) UNSIGNED DEFAULT NULL,
        UNIQUE INDEX _index1 (_document, _tenant, _type, _permission),
        INDEX _permission (_tenant, _permission, _type)
      `;
    } else {
      permissionsSql += `
        UNIQUE INDEX _index1 (_document, _type, _permission),
        INDEX _permission (_permission, _type)
      `;
    }

    permissionsSql += `)`;

    try {
      await this.pool.query<any>(permissionsSql);
      const [result] = await this.pool.query<any>(collectionSql);

      this.logger.debug(result);
      return true;
    } catch (e) {
      this.logger.error(e);
      throw new DatabaseError("Table Creation Failed.");
    }
  }


  /**
   * Converts a given type, size, and other attributes into a corresponding SQL type string.
   * Supports various data types including strings, integers, floats, booleans, datetimes, and JSON.
   * Handles special cases for array types and unsigned integers.
   * 
   * @param type - The data type to be converted.
   * @param size - The size of the data type.
   * @param signed - Indicates if the integer type is signed or unsigned.
   * @param array - Indicates if the type is an array.
   * @returns The corresponding SQL type string.
   */
  private getSqlType(type: string, size: number, signed: boolean = true, array: boolean = false): string {

    if (array) {
      return `JSON`
    }

    switch (type) {
      case Database.VAR_STRING:
        if (size === 0) {
          return `TEXT`
        }

        if (size > 16777215) {
          return `LONGTEXT`;
        }

        if (size > 65535) {
          return 'MEDIUMTEXT';
        }

        return `VARCHAR(${size})`
      case 'text':
        return `TEXT`
      case Database.VAR_INTEGER:
        signed = signed ? '' : ' UNSIGNED' as any;

        if (size >= 8) {
          return `BIGINT` + signed;
        }

        return `INT(${size})` + signed;
      case Database.VAR_FLOAT:
        signed = signed ? '' : ' UNSIGNED' as any;
        return `DOUBLE(${size})` + signed;
      case Database.VAR_BOOLEAN:
        return `BOOLEAN`
      case Database.VAR_DATETIME:
        return `DATETIME(3)`
      case 'json':
        return `JSON`
      default:
        return `VARCHAR(${size})`;
    }
  }

}