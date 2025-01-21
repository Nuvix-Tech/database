import { Constant as Database } from "../core/constant";
import { Document } from "../core/Document";
import { Query } from "../core/query";
import { Attribute } from "../core/types/attribute";
import { Index, IndexType } from "../core/types/indexes";
import { InitializeError } from "../errors/adapter";
import { DatabaseError } from "../errors/base";
import { Authorization } from "../security/authorization";
import { Adapter } from "./base";
import * as mysql2 from 'mysql2/promise';
import { Sql } from "./sql";
import { DuplicateException, NotFoundException, TimeoutException, TruncateException } from "../errors";

interface MariaDBOptions {
  connection: mysql2.PoolOptions;
}

/**
 * MariaDB adapter class
 */
export class MariaDB extends Sql implements Adapter {
  /**
   * MariaDB pool / connection
   */
  // @ts-ignore
  pool: mysql2.Pool;

  /**
   * MariaDB adapter library
   */
  library: typeof mysql2;

  /**
   * MariaDB adapter instance
   */
  instance: this | null = null;

  /**
   * MariaDB adapter options
   */
  options: MariaDBOptions;

  constructor(options: MariaDBOptions) {
    super()
    this.library = this.loadModule('mysql2/promise');
    this.type = 'mariadb';
    this.options = options;

    if (!this.database) {
      this.database = options.connection.database || this.database;
    } else if (!options.connection.database) {
      options.connection.database = this.database;
    }
  }

  /**
   * Initialize MariaDB adapter
   * @throws {InitializeError} If adapter is already initialized
   */
  async init() {
    if (this.instance) throw new InitializeError('MariaDB adapter already initialized');
    if (!this.options.connection.database) throw new InitializeError('Database name is required');

    try {
      const pool = this.library.createPool({
        ...this.options.connection,
        namedPlaceholders: true
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

  public isInitialized() {
    return this.instance ? true : false;
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

  getConnectionId(): string | number {
    return this.pool.threadId
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
  private getTenantSql() {
    return `AND (_tenant = ? OR _tenant IS NULL)`
  }

  /**
   * Generates an SQL condition string based on the provided query.
   *
   * @param {Query} query - The query object containing the condition details.
   * @returns {string} The SQL condition string.
   *
   * The method processes the query object to generate the appropriate SQL condition string.
   * It handles various query types such as OR, AND, SEARCH, BETWEEN, IS_NULL, IS_NOT_NULL, and CONTAINS.
   * 
   * - For OR and AND types, it recursively generates conditions for each sub-query and combines them.
   * - For SEARCH type, it generates a MATCH condition.
   * - For BETWEEN type, it generates a BETWEEN condition.
   * - For IS_NULL and IS_NOT_NULL types, it generates the respective SQL condition.
   * - For CONTAINS type, if JSON_OVERLAPS is supported and the query is on an array, it generates a JSON_OVERLAPS condition.
   * - For other types, it generates conditions based on the query values and method.
   *
   * The method also maps certain attribute names to their corresponding database column names using the `attributeMap`.
   */
  public getSQLCondition(query: Query): string {
    const attributeMap: { [key: string]: string } = {
      '$id': '_uid',
      '$internalId': '_id',
      '$tenant': '_tenant',
      '$createdAt': '_createdAt',
      '$updatedAt': '_updatedAt'
    };
    query.setAttribute(attributeMap[query.getAttribute()] || query.getAttribute());

    const attribute = `\`${query.getAttribute()}\``;
    // const placeholder = this.getSQLPlaceholder(query);

    switch (query.getMethod()) {
      case Query.TYPE_OR:
      case Query.TYPE_AND:
        const conditions = query.getValue().map((q: Query) => this.getSQLCondition(q));
        const method = query.getMethod().toUpperCase();
        return conditions.length ? ` ${method} (${conditions.join(' AND ')})` : '';

      case Query.TYPE_SEARCH:
        return `MATCH(\`table_main\`.${attribute}) AGAINST (? IN BOOLEAN MODE)`; // :${placeholder}_0

      case Query.TYPE_BETWEEN:
        return `\`table_main\`.${attribute} BETWEEN ? AND ?`; // :${placeholder}_1

      case Query.TYPE_IS_NULL:
      case Query.TYPE_IS_NOT_NULL:
        return `\`table_main\`.${attribute} ${this.getSQLOperator(query.getMethod())}`;
      //@ts-ignore
      case Query.TYPE_CONTAINS:
        if (this.getSupportForJSONOverlaps() && query.onArray()) {
          return `JSON_OVERLAPS(\`table_main\`.${attribute}, ?)`;
        }
      // fallthrough
      default:
        const defaultConditions = query.getValues().map((value, key) => {
          return `${attribute} ${this.getSQLOperator(query.getMethod())} ?`;// :${placeholder}_${key}
        });
        return defaultConditions.length ? `(${defaultConditions.join(' OR ')})` : '';
    }
  }

  /**
   * Checks if the database adapter supports the JSON_OVERLAPS function.
   *
   * @returns {boolean} `true` if JSON_OVERLAPS is supported, otherwise `false`.
   */
  public getSupportForJSONOverlaps(): boolean {
    return true;
  }

  /**
   * Create Database
   */
  async create(name: string): Promise<boolean> {
    try {
      await this.pool.query<any>(`CREATE SCHEMA IF NOT EXISTS \`${name}\``);
      return true;
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
      await this.pool.query<any>(`DROP SCHEMA IF EXISTS \`${name}\``);
      return true;
    } catch (e) {
      this.logger.error(e)
      throw new DatabaseError("Database Drop Failed.")
    }
  }

  async exists(name: string, collection?: string): Promise<boolean> {
    return true;
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
  public async createCollection(name: string, attributes: Document[], indexes: Document[], ifExists: boolean = false): Promise<boolean> {
    const attributeSql = attributes.map((attribute) => {
      const attrId = this.filter(attribute.getAttribute("key", attribute.getAttribute("$id")));
      const attrType = this.getSqlType(
        attribute.getAttribute("type"),
        attribute.getAttribute("size", 0),
        attribute.getAttribute("signed", false),
        attribute.getAttribute("array", false)
      );

      // Ignore relationships with virtual attributes
      if (attribute.getAttribute("type") === Database.VAR_RELATIONSHIP) {
        const { relationType, twoWay, side } = attribute.getAttribute("options") || {};

        if (
          relationType === Database.RELATION_MANY_TO_MANY ||
          (relationType === Database.RELATION_ONE_TO_ONE && !twoWay && side === Database.RELATION_SIDE_CHILD) ||
          (relationType === Database.RELATION_ONE_TO_MANY && side === Database.RELATION_SIDE_PARENT) ||
          (relationType === Database.RELATION_MANY_TO_ONE && side === Database.RELATION_SIDE_CHILD)
        ) {
          return '';
        }
      }

      return `\`${attrId}\` ${attrType}`;
    }).filter(Boolean).join(', ');

    const indexSql = indexes.map((index) => {
      const indexId = this.filter(index.getAttribute("name", index.getAttribute("$id")));
      const indexType = index.getAttribute("type");

      const indexAttributes = index.getAttribute("attributes", [])?.map((attribute: string, nested: number) => {
        let indexLength = index.getAttribute("lengths", [])?.[nested] ?? '';
        indexLength = indexLength ? `(${indexLength})` : '';
        let indexOrder = index.getAttribute("orders", [])?.[nested] ?? '';

        let indexAttribute = attribute;
        if (attribute === '$id') indexAttribute = '_uid';
        if (attribute === '$createdAt') indexAttribute = '_createdAt';
        if (attribute === '$updatedAt') indexAttribute = '_updatedAt';

        if (indexType === IndexType.FULLTEXT) indexOrder = '';

        return `\`${indexAttribute}\`${indexLength} ${indexOrder}`;
      }).join(', ');

      if (this.sharedTables && indexType !== IndexType.FULLTEXT) {
        return `${indexType} \`${indexId}\` (_tenant, ${indexAttributes})`;
      }

      return `${indexType} \`${indexId}\` (${indexAttributes})`;
    }).join(', ');

    ifExists = ifExists ? 'IF NOT EXISTS' : '' as any;

    let collectionSql = `
      CREATE TABLE ${ifExists} ${this.getSqlTable(name)} (
        _id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT,
        _uid VARCHAR(255) NOT NULL,
        _createdAt DATETIME(3) DEFAULT NULL,
        _updatedAt DATETIME(3) DEFAULT NULL,
        _permissions JSON DEFAULT NULL,
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
      CREATE TABLE ${ifExists} ${this.getSqlTable(name + '_perms')} (
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
   * Drops a collection from the database if it exists.
   *
   * @param name - The name of the collection to drop.
   * @param ifExists - A boolean flag indicating whether to include the "IF EXISTS" clause in the SQL statement. Defaults to false.
   * @returns A promise that resolves to a boolean indicating whether the collection was successfully dropped.
   * @throws {DatabaseError} If the collection drop operation fails.
   */
  public async dropCollection(name: string, ifExists: boolean = false): Promise<boolean> {
    ifExists = ifExists ? 'IF EXISTS' : '' as any;

    let sql = `DROP TABLE ${ifExists} ${this.getSqlTable(name)}, ${this.getSqlTable(name + '_perms')}`;

    try {
      const [result] = await this.pool.query<any>(sql);

      this.logger.debug(result);
      return true;
    } catch (e) {
      this.logger.error(e);
      throw new DatabaseError("Table Drop Failed.");
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


  /**
   * Create a new attribute in the specified collection.
   * 
   * @param collection - The name of the collection.
   * @param id - The name of the attribute.
   * @param type - The data type of the attribute.
   * @param size - The size of the attribute.
   * @param signed - Indicates if the integer type is signed or unsigned.
   * @param array - Indicates if the type is an array.
   * @returns A promise that resolves to a boolean indicating whether the attribute was successfully created.
   * @throws {DatabaseError} If the attribute creation fails.
   */
  public async createAttribute(collection: string, id: string, type: string, size: number, signed: boolean = true, array: boolean = false): Promise<boolean> {
    const name = this.filter(collection);
    const attributeId = this.filter(id);
    const sqlType = this.getSqlType(type, size, signed, array);

    let sql = `ALTER TABLE ${this.getSqlTable(name)} ADD COLUMN \`${attributeId}\` ${sqlType};`;
    sql = this.trigger(Database.EVENT_ATTRIBUTE_CREATE, sql)

    try {
      const [result] = await this.pool.query<any>(sql);
      this.logger.debug(result);
      return true;
    } catch (e) {
      this.logger.error(e);
      throw new DatabaseError("Attribute Creation Failed.");
    }
  }

  /**
   * Update an attribute in the specified collection.
   * 
   * @param collection - The name of the collection.
   * @param id - The name of the attribute.
   * @param type - The data type of the attribute.
   * @param size - The size of the attribute.
   * @param signed - Indicates if the integer type is signed or unsigned.
   * @param array - Indicates if the type is an array.
   * @param newKey - The new name of the attribute, if it needs to be renamed.
   * @returns A promise that resolves to a boolean indicating whether the attribute was successfully updated.
   * @throws {DatabaseError} If the attribute update fails.
   */
  public async updateAttribute(collection: string, id: string, type: string, size: number, signed: boolean = true, array: boolean = false, newKey?: string): Promise<boolean> {
    const name = this.filter(collection);
    const attributeId = this.filter(id);
    const newAttributeId = newKey ? this.filter(newKey) : null;
    const sqlType = this.getSqlType(type, size, signed, array);

    let sql;
    if (newAttributeId) {
      sql = `ALTER TABLE ${this.getSqlTable(name)} CHANGE COLUMN \`${attributeId}\` \`${newAttributeId}\` ${sqlType};`;
    } else {
      sql = `ALTER TABLE ${this.getSqlTable(name)} MODIFY \`${attributeId}\` ${sqlType};`;
    }

    sql = this.trigger(Database.EVENT_ATTRIBUTE_UPDATE, sql)

    try {
      const [result] = await this.pool.query<any>(sql);
      this.logger.debug(result);
      return true;
    } catch (e) {
      this.logger.error(e);
      throw new DatabaseError("Attribute Update Failed.");
    }
  }

  /**
   * Delete an attribute from the specified collection.
   * 
   * @param collection - The name of the collection.
   * @param id - The name of the attribute.
   * @returns A promise that resolves to a boolean indicating whether the attribute was successfully deleted.
   * @throws {DatabaseError} If the attribute deletion fails.
   */
  public async deleteAttribute(collection: string, id: string): Promise<boolean> {
    const name = this.filter(collection);
    const attributeId = this.filter(id);

    let sql = `ALTER TABLE ${this.getSqlTable(name)} DROP COLUMN \`${attributeId}\`;`;
    sql = this.trigger(Database.EVENT_ATTRIBUTE_DELETE, sql)

    try {
      const [result] = await this.pool.query<any>(sql);
      this.logger.debug(result);
      return true;
    } catch (e) {
      this.logger.error(e);
      throw new DatabaseError("Attribute Deletion Failed.");
    }
  }

  /**
   * Rename an attribute in the specified collection.
   * 
   * @param collection - The name of the collection.
   * @param oldName - The current name of the attribute.
   * @param newName - The new name of the attribute.
   * @returns A promise that resolves to a boolean indicating whether the attribute was successfully renamed.
   * @throws {DatabaseError} If the attribute rename fails.
   */
  public async renameAttribute(collection: string, oldName: string, newName: string): Promise<boolean> {
    const name = this.filter(collection);
    const oldAttributeName = this.filter(oldName);
    const newAttributeName = this.filter(newName);

    let sql = `ALTER TABLE ${this.getSqlTable(name)} RENAME COLUMN \`${oldAttributeName}\` TO \`${newAttributeName}\`;`;
    sql = this.trigger(Database.EVENT_ATTRIBUTE_UPDATE, sql);

    try {
      const [result] = await this.pool.query<any>(sql);
      this.logger.debug(result);
      return true;
    } catch (e) {
      this.logger.error(e);
      throw new DatabaseError("Attribute Rename Failed.");
    }
  }

  /**
   * Create a relationship between collections.
   * 
   * @param collection - The name of the collection.
   * @param relatedCollection - The name of the related collection.
   * @param type - The type of relationship.
   * @param twoWay - Indicates if the relationship is two-way.
   * @param id - The name of the relationship attribute.
   * @param twoWayKey - The name of the two-way relationship attribute.
   * @returns A promise that resolves to a boolean indicating whether the relationship was successfully created.
   * @throws {DatabaseError} If the relationship creation fails.
   */
  public async createRelationship(collection: string, relatedCollection: string, type: string, twoWay: boolean = false, id: string = '', twoWayKey: string = ''): Promise<boolean> {
    const name = this.filter(collection);
    const relatedName = this.filter(relatedCollection);
    const table = this.getSqlTable(name);
    const relatedTable = this.getSqlTable(relatedName);
    id = this.filter(id);
    twoWayKey = this.filter(twoWayKey);
    const sqlType = this.getSqlType(Database.VAR_RELATIONSHIP, 0, false);

    let sql = '';

    switch (type) {
      case Database.RELATION_ONE_TO_ONE:
        sql = `ALTER TABLE ${table} ADD COLUMN \`${id}\` ${sqlType} DEFAULT NULL;`;
        if (twoWay) {
          sql += `ALTER TABLE ${relatedTable} ADD COLUMN \`${twoWayKey}\` ${sqlType} DEFAULT NULL;`;
        }
        break;
      case Database.RELATION_ONE_TO_MANY:
        sql = `ALTER TABLE ${relatedTable} ADD COLUMN \`${twoWayKey}\` ${sqlType} DEFAULT NULL;`;
        break;
      case Database.RELATION_MANY_TO_ONE:
        sql = `ALTER TABLE ${table} ADD COLUMN \`${id}\` ${sqlType} DEFAULT NULL;`;
        break;
      case Database.RELATION_MANY_TO_MANY:
        return true;
      default:
        throw new DatabaseError('Invalid relationship type');
    }

    sql = this.trigger(Database.EVENT_ATTRIBUTE_CREATE, sql);

    try {
      const [result] = await this.pool.query<any>(sql);
      this.logger.debug(result);
      return true;
    } catch (e) {
      this.logger.error(e);
      throw new DatabaseError("Relationship Creation Failed.");
    }
  }

  /**
   * Update a relationship between collections.
   * 
   * @param collection - The name of the collection.
   * @param relatedCollection - The name of the related collection.
   * @param type - The type of relationship.
   * @param twoWay - Indicates if the relationship is two-way.
   * @param key - The name of the relationship attribute.
   * @param twoWayKey - The name of the two-way relationship attribute.
   * @param side - The side of the relationship.
   * @param newKey - The new name of the relationship attribute, if it needs to be renamed.
   * @param newTwoWayKey - The new name of the two-way relationship attribute, if it needs to be renamed.
   * @returns A promise that resolves to a boolean indicating whether the relationship was successfully updated.
   * @throws {DatabaseError} If the relationship update fails.
   */
  public async updateRelationship(collection: string, relatedCollection: string, type: string, twoWay: boolean, key: string, twoWayKey: string, side: string, newKey?: string, newTwoWayKey?: string): Promise<boolean> {
    const name = this.filter(collection);
    const relatedName = this.filter(relatedCollection);
    const table = this.getSqlTable(name);
    const relatedTable = this.getSqlTable(relatedName);
    key = this.filter(key);
    twoWayKey = this.filter(twoWayKey);

    if (newKey) {
      newKey = this.filter(newKey);
    }
    if (newTwoWayKey) {
      newTwoWayKey = this.filter(newTwoWayKey);
    }

    let sql = '';

    switch (type) {
      case Database.RELATION_ONE_TO_ONE:
        if (key !== newKey) {
          sql = `ALTER TABLE ${table} RENAME COLUMN \`${key}\` TO \`${newKey}\`;`;
        }
        if (twoWay && twoWayKey !== newTwoWayKey) {
          sql += `ALTER TABLE ${relatedTable} RENAME COLUMN \`${twoWayKey}\` TO \`${newTwoWayKey}\`;`;
        }
        break;
      case Database.RELATION_ONE_TO_MANY:
        if (side === Database.RELATION_SIDE_PARENT) {
          if (twoWayKey !== newTwoWayKey) {
            sql = `ALTER TABLE ${relatedTable} RENAME COLUMN \`${twoWayKey}\` TO \`${newTwoWayKey}\`;`;
          }
        } else {
          if (key !== newKey) {
            sql = `ALTER TABLE ${table} RENAME COLUMN \`${key}\` TO \`${newKey}\`;`;
          }
        }
        break;
      case Database.RELATION_MANY_TO_ONE:
        if (side === Database.RELATION_SIDE_CHILD) {
          if (twoWayKey !== newTwoWayKey) {
            sql = `ALTER TABLE ${relatedTable} RENAME COLUMN \`${twoWayKey}\` TO \`${newTwoWayKey}\`;`;
          }
        } else {
          if (key !== newKey) {
            sql = `ALTER TABLE ${table} RENAME COLUMN \`${key}\` TO \`${newKey}\`;`;
          }
        }
        break;
      case Database.RELATION_MANY_TO_MANY:
        const collectionDoc = await this.getDocument(Database.METADATA, collection);
        const relatedCollectionDoc = await this.getDocument(Database.METADATA, relatedCollection);

        const junction = this.getSqlTable(`_${collectionDoc.getInternalId()}_${relatedCollectionDoc.getInternalId()}`);

        if (newKey) {
          sql = `ALTER TABLE ${junction} RENAME COLUMN \`${key}\` TO \`${newKey}\`;`;
        }
        if (twoWay && newTwoWayKey) {
          sql += `ALTER TABLE ${junction} RENAME COLUMN \`${twoWayKey}\` TO \`${newTwoWayKey}\`;`;
        }
        break;
      default:
        throw new DatabaseError('Invalid relationship type');
    }

    if (!sql) {
      return true;
    }

    sql = this.trigger(Database.EVENT_ATTRIBUTE_UPDATE, sql);

    try {
      const [result] = await this.pool.query<any>(sql);
      this.logger.debug(result);
      return true;
    } catch (e) {
      this.logger.error(e);
      throw new DatabaseError("Relationship Update Failed.");
    }
  }

  /**
   * Delete a relationship between collections.
   * 
   * @param collection - The name of the collection.
   * @param relatedCollection - The name of the related collection.
   * @param type - The type of relationship.
   * @param twoWay - Indicates if the relationship is two-way.
   * @param key - The name of the relationship attribute.
   * @param twoWayKey - The name of the two-way relationship attribute.
   * @param side - The side of the relationship.
   * @returns A promise that resolves to a boolean indicating whether the relationship was successfully deleted.
   * @throws {DatabaseError} If the relationship deletion fails.
   */
  public async deleteRelationship(collection: string, relatedCollection: string, type: string, twoWay: boolean, key: string, twoWayKey: string, side: string): Promise<boolean> {
    const name = this.filter(collection);
    const relatedName = this.filter(relatedCollection);
    const table = this.getSqlTable(name);
    const relatedTable = this.getSqlTable(relatedName);
    key = this.filter(key);
    twoWayKey = this.filter(twoWayKey);

    let sql = '';

    switch (type) {
      case Database.RELATION_ONE_TO_ONE:
        if (side === Database.RELATION_SIDE_PARENT) {
          sql = `ALTER TABLE ${table} DROP COLUMN \`${key}\`;`;
          if (twoWay) {
            sql += `ALTER TABLE ${relatedTable} DROP COLUMN \`${twoWayKey}\`;`;
          }
        } else if (side === Database.RELATION_SIDE_CHILD) {
          sql = `ALTER TABLE ${relatedTable} DROP COLUMN \`${twoWayKey}\`;`;
          if (twoWay) {
            sql += `ALTER TABLE ${table} DROP COLUMN \`${key}\`;`;
          }
        }
        break;
      case Database.RELATION_ONE_TO_MANY:
        if (side === Database.RELATION_SIDE_PARENT) {
          sql = `ALTER TABLE ${relatedTable} DROP COLUMN \`${twoWayKey}\`;`;
        } else {
          sql = `ALTER TABLE ${table} DROP COLUMN \`${key}\`;`;
        }
        break;
      case Database.RELATION_MANY_TO_ONE:
        if (side === Database.RELATION_SIDE_PARENT) {
          sql = `ALTER TABLE ${table} DROP COLUMN \`${key}\`;`;
        } else {
          sql = `ALTER TABLE ${relatedTable} DROP COLUMN \`${twoWayKey}\`;`;
        }
        break;
      case Database.RELATION_MANY_TO_MANY:
        const collectionDoc = await this.getDocument(Database.METADATA, collection);
        const relatedCollectionDoc = await this.getDocument(Database.METADATA, relatedCollection);

        const junction = side === Database.RELATION_SIDE_PARENT
          ? this.getSqlTable(`_${collectionDoc.getInternalId()}_${relatedCollectionDoc.getInternalId()}`)
          : this.getSqlTable(`_${relatedCollectionDoc.getInternalId()}_${collectionDoc.getInternalId()}`);

        const perms = side === Database.RELATION_SIDE_PARENT
          ? this.getSqlTable(`_${collectionDoc.getInternalId()}_${relatedCollectionDoc.getInternalId()}_perms`)
          : this.getSqlTable(`_${relatedCollectionDoc.getInternalId()}_${collectionDoc.getInternalId()}_perms`);

        sql = `DROP TABLE ${junction}; DROP TABLE ${perms}`;
        break;
      default:
        throw new DatabaseError('Invalid relationship type');
    }

    if (!sql) {
      return true;
    }

    sql = this.trigger(Database.EVENT_ATTRIBUTE_DELETE, sql);

    try {
      const [result] = await this.pool.query<any>(sql);
      this.logger.debug(result);
      return true;
    } catch (e) {
      this.logger.error(e);
      throw new DatabaseError("Relationship Deletion Failed.");
    }
  }

  /**
   * Rename Index
   *
   * @param collection - The name of the collection.
   * @param oldName - The current name of the index.
   * @param newName - The new name of the index.
   * @returns A promise that resolves to a boolean indicating whether the index was successfully renamed.
   * @throws {DatabaseError} If the index rename fails.
   */
  public async renameIndex(collection: string, oldName: string, newName: string): Promise<boolean> {
    const name = this.filter(collection);
    const oldIndexName = this.filter(oldName);
    const newIndexName = this.filter(newName);

    let sql = `ALTER TABLE ${this.getSqlTable(name)} RENAME INDEX \`${oldIndexName}\` TO \`${newIndexName}\`;`;
    sql = this.trigger(Database.EVENT_INDEX_RENAME, sql);

    try {
      const [result] = await this.pool.query<any>(sql);
      this.logger.debug(result);
      return true;
    } catch (e) {
      this.logger.error(e);
      throw new DatabaseError("Index Rename Failed.");
    }
  }

  /**
   * Create Index
   *
   * @param collection - The name of the collection.
   * @param id - The name of the index.
   * @param type - The type of the index.
   * @param attributes - An array of attributes to be indexed.
   * @param lengths - An array of lengths for the indexed attributes.
   * @param orders - An array of orders for the indexed attributes.
   * @returns A promise that resolves to a boolean indicating whether the index was successfully created.
   * @throws {DatabaseError} If the index creation fails.
   */
  public async createIndex(collection: string, id: string, type: string, attributes: string[], lengths: number[], orders: string[]): Promise<boolean> {
    const name = this.filter(collection);
    const indexId = this.filter(id);

    let indexAttributes = attributes.map((attribute, i) => {
      let length = lengths[i] ? `(${lengths[i]})` : '';
      let order = orders[i] ? ` ${orders[i]}` : '';

      attribute = this.filter(attribute);
      return `\`${attribute}\`${length}${order}`;
    }).join(', ');

    if (this.sharedTables && type !== IndexType.FULLTEXT) {
      indexAttributes = `_tenant, ${indexAttributes}`;
    }

    let sqlType;
    switch (type) {
      case IndexType.KEY:
        sqlType = 'INDEX';
        break;
      case IndexType.UNIQUE:
        sqlType = 'UNIQUE INDEX';
        break;
      case IndexType.FULLTEXT:
        sqlType = 'FULLTEXT INDEX';
        break;
      default:
        throw new DatabaseError('Unknown index type');
    }

    let sql = `CREATE ${sqlType} \`${indexId}\` ON ${this.getSqlTable(name)} (${indexAttributes})`;
    sql = this.trigger(Database.EVENT_INDEX_CREATE, sql);

    try {
      const [result] = await this.pool.query<any>(sql);
      this.logger.debug(result);
      return true;
    } catch (e) {
      this.logger.error(e);
      throw new DatabaseError("Index Creation Failed.");
    }
  }

  /**
   * Delete Index
   *
   * @param collection - The name of the collection.
   * @param id - The name of the index.
   * @returns A promise that resolves to a boolean indicating whether the index was successfully deleted.
   * @throws {DatabaseError} If the index deletion fails.
   */
  public async deleteIndex(collection: string, id: string): Promise<boolean> {
    const name = this.filter(collection);
    const indexId = this.filter(id);

    let sql = `ALTER TABLE ${this.getSqlTable(name)} DROP INDEX \`${indexId}\`;`;
    sql = this.trigger(Database.EVENT_INDEX_DELETE, sql);

    try {
      const [result] = await this.pool.query<any>(sql);
      this.logger.debug(result);
      return true;
    } catch (e) {
      this.logger.error(e);
      throw new DatabaseError("Index Deletion Failed.");
    }
  }

  /**
   * Create a new document in the specified collection.
   * 
   * @param collection - The name of the collection.
   * @param document - The document to be created.
   * @returns A promise that resolves to a boolean indicating whether the document was successfully created.
   * @throws {DatabaseError} If the document creation fails.
   */
  public async createDocument(collection: string, document: Document): Promise<Document> {
    const name = this.filter(collection);
    let attributes = document.getAttributes();
    attributes._createdAt = document.getCreatedAt();
    attributes._updatedAt = document.getUpdatedAt();
    attributes._permissions = document.getPermissions();

    let columns: string[] = [];
    let values: any[] = [];
    let placeholders: string[] = [];

    // Process attributes
    Object.entries(attributes).forEach(([attribute, value], index) => {
      if (Database.INTERNAL_ATTRIBUTES.map((v) => v.$id).includes(attribute)) return;
      const column = this.filter(attribute);
      columns.push(`\`${column}\``);
      placeholders.push('?');
      values.push(Array.isArray(value) || typeof value === 'object' ? JSON.stringify(value) : value);
    });

    if (document.getInternalId?.()) {
      columns.push('_id');
      placeholders.push('?');
      values.push(document.getInternalId());
    }

    columns.push('_uid');
    placeholders.push('?');
    values.push(document.getId());

    if (this.sharedTables) {
      columns.push('_tenant');
      placeholders.push('?');
      values.push(this.tenantId);
    }

    let sql = `
    INSERT INTO ${this.getSqlTable(name)} 
    (${columns.join(', ')})
    VALUES (${placeholders.join(', ')})
    `;

    sql = this.trigger(Database.EVENT_DOCUMENT_CREATE, sql);

    try {
      const [result] = await this.pool.query<any>(sql, values);

      // Handle permissions if any
      if (document.getPermissions()) {
        const permValues: any[] = [];
        const permPlaceholders: string[] = [];

        Database.PERMISSIONS.forEach(type => {
          document.getPermissionsByType(type)?.forEach((permission: string) => {
            permPlaceholders.push('(?, ?, ?' + (this.sharedTables ? ', ?' : '') + ')');
            permValues.push(
              type,
              permission.replace(/"/g, ''),
              document.getId(),
              ...(this.sharedTables ? [this.tenantId] : [])
            );
          });
        });

        if (permValues.length) {
          let permSql = `
                INSERT INTO ${this.getSqlTable(name + '_perms')}
                (_type, _permission, _document${this.sharedTables ? ', _tenant' : ''})
                VALUES ${permPlaceholders.join(', ')}
            `;
          permSql = this.trigger(Database.EVENT_PERMISSIONS_CREATE, permSql);
          await this.pool.query(permSql, permValues);
        }
      }

      this.logger.debug(result);

      document.set("$internalId", result.insertId);
      return document;
    } catch (e) {
      this.logger.error(e);
      throw new DatabaseError("Document Creation Failed");
    }
  }

  /**
   * Create Documents in batches
   *
   * @param collection - The name of the collection.
   * @param documents - An array of documents to be created.
   * @param batchSize - The size of each batch. Defaults to 1000.
   * @returns A promise that resolves to an array of created documents.
   * @throws {DatabaseError} If the document creation fails.
   */
  public async createDocuments(collection: string, documents: Document[], batchSize: number = 1000): Promise<Document[]> {
    if (documents.length === 0) {
      return documents;
    }

    const name = this.filter(collection);
    const batches = this.chunkArray(documents, batchSize);
    const internalIds: { [key: string]: boolean } = {};

    for (const batch of batches) {
      const bindValues: any[] = [];
      const permissions: any[] = [];
      const columns: string[] = [];
      const placeholders: string[] = [];

      batch.forEach((document, index) => {
        const attributes = document.getAttributes();
        attributes._uid = document.getId();
        attributes._createdAt = document.getCreatedAt();
        attributes._updatedAt = document.getUpdatedAt();
        attributes._permissions = JSON.stringify(document.getPermissions());

        if (document.getInternalId()) {
          internalIds[document.getId()] = true;
          attributes._id = document.getInternalId();
        }

        if (this.sharedTables) {
          attributes._tenant = this.tenantId;
        }

        if (index === 0) {
          columns.push(...Object.keys(attributes)
            .filter(attr => !Database.INTERNAL_ATTRIBUTES.map((v) => v.$id).includes(attr))
            .map(attr => `\`${this.filter(attr)}\``));
        }

        const filteredAttributes = Object.entries(attributes)
          .filter(([attr]) => !Database.INTERNAL_ATTRIBUTES.map((v) => v.$id).includes(attr))
          .reduce((obj, [key, value]) => ({ ...obj, [key]: value }), {});

        placeholders.push(`(${Object.keys(filteredAttributes).map(() => '?').join(', ')})`);
        bindValues.push(...Object.values(filteredAttributes).map(value =>
          Array.isArray(value) || typeof value === 'object' ? JSON.stringify(value) : value
        ));

        Database.PERMISSIONS.forEach(type => {
          document.getPermissionsByType(type)?.forEach(permission => {
            permissions.push(`('${type}', '${permission.replace(/"/g, '')}', '${document.getId()}'${this.sharedTables ? `, ${this.tenantId}` : ''})`);
          });
        });
      });

      const sql = `
        INSERT INTO ${this.getSqlTable(name)} (${columns.join(', ')})
        VALUES ${placeholders.join(', ')}
      `;

      try {
        await this.pool.query<any>(sql, bindValues);

        if (permissions.length > 0) {
          const permSql = `
            INSERT INTO ${this.getSqlTable(name + '_perms')} (_type, _permission, _document${this.sharedTables ? ', _tenant' : ''})
            VALUES ${permissions.join(', ')}
          `;
          await this.pool.query<any>(permSql);
        }
      } catch (e) {
        this.logger.error(e);
        throw new DatabaseError("Batch Document Creation Failed.");
      }
    }

    for (const document of documents) {
      if (!internalIds[document.getId()]) {
        const doc = await this.getDocument(collection, document.getId());
        document.set("$internalId", doc.getInternalId());
      }
    }

    return documents;
  }

  /**
   * Helper function to chunk an array into smaller arrays of a specified size.
   *
   * @param array - The array to be chunked.
   * @param size - The size of each chunk.
   * @returns An array of chunked arrays.
   */
  private chunkArray<T>(array: T[], size: number): T[][] {
    const result: T[][] = [];
    for (let i = 0; i < array.length; i += size) {
      result.push(array.slice(i, i + size));
    }
    return result;
  }

  /**
   * Update a document in the specified collection.
   * 
   * @param collection - The name of the collection.
   * @param document - The document to be updated.
   * @returns A promise that resolves to a boolean indicating whether the document was successfully updated.
   * @throws {DatabaseError} If the document update fails.
   */
  public async updateDocument(collection: string, id: string, document: Document): Promise<Document> {
    const name = this.filter(collection);

    if (!id) throw new DatabaseError("Document ID is required.");

    const attributes = document.getAttributes();
    if (document.getCreatedAt()) {
      attributes._createdAt = document.getCreatedAt();
    }
    if (document.getUpdatedAt()) {
      attributes._updatedAt = document.getUpdatedAt();
    }
    if (document.getPermissions()) {
      attributes._permissions = document.getPermissions();
    }

    delete attributes.$permissions;

    if (this.sharedTables) {
      attributes._tenant = this.tenantId;
    }

    const columns = Object.keys(attributes)
      .filter(attr => !Database.INTERNAL_ATTRIBUTES.map((v) => v.$id).includes(attr))
      .map(attr => `\`${this.filter(attr)}\` = ?`)
      .join(', ');

    const values = Object.keys(attributes)
      .filter(attr => !Database.INTERNAL_ATTRIBUTES.map((v) => v.$id).includes(attr))
      .map(attr => {
        const value = attributes[attr];
        return Array.isArray(value) || typeof value === 'object' ? JSON.stringify(value) : value;
      }) ?? [];

    let sql = `UPDATE ${this.getSqlTable(name)} SET ${columns} WHERE _uid = ?`;
    values.push(id);

    sql = this.trigger(Database.EVENT_DOCUMENT_UPDATE, sql);

    try {
      await this.pool.query<any>(sql, values);

      // Handle permissions if any
      if (document.getPermissions()) {
        const currentPermissions = await this.getCurrentPermissions(name, id);
        const { additions, removals } = this.getPermissionChanges(currentPermissions, document.getPermissions());

        if (Object.values(removals).some((perms: any) => perms.length > 0)) {
          await this.removePermissions(name, id, removals);
        }

        if (Object.values(additions).some((perms: any) => perms.length > 0)) {
          await this.addPermissions(name, id, additions);
        }
      }

      return document;
    } catch (e) {
      this.logger.error(e);
      throw new DatabaseError("Document Update Failed.");
    }
  }

  /**
   * Update documents
   *
   * Updates all documents which match the given query.
   *
   * @param collection - The name of the collection.
   * @param updates - The document containing the updates.
   * @param documents - An array of documents to be updated.
   * @returns A promise that resolves to the number of documents successfully updated.
   * @throws {DatabaseError} If the document update fails.
   */
  public async updateDocuments(collection: string, updates: Document, documents: Document[]): Promise<number> {
    const attributes = updates.getAttributes();

    if (updates.getUpdatedAt()) {
      attributes['_updatedAt'] = updates.getUpdatedAt();
    }

    if (updates.getPermissions()) {
      attributes['_permissions'] = JSON.stringify(updates.getPermissions());
    }

    if (Object.keys(attributes).length === 0) {
      return 0;
    }

    const name = this.filter(collection);
    const ids = documents.map(doc => doc.getId());
    const where = [`_uid IN (${ids.map(() => '?').join(', ')})`];

    // First collect update values
    const updateColumns = Object.keys(attributes)
      .filter(attr => !Database.INTERNAL_ATTRIBUTES.map((v) => v.$id).includes(attr))
      .map((attr) => `\`${this.filter(attr)}\` = ?`)
      .join(', ');

    const updateValues = Object.keys(attributes)
      .filter(attr => !Database.INTERNAL_ATTRIBUTES.map((v) => v.$id).includes(attr))
      .map(attr => {
        const value = attributes[attr];
        return Array.isArray(value) || typeof value === 'object' ? JSON.stringify(value) : value;
      });

    // Then collect WHERE clause values
    const values: any[] = [...updateValues, ...ids];

    if (this.sharedTables) {
      where.push('_tenant = ?');
      values.push(this.tenantId);
    }

    const sqlWhere = `WHERE ${where.join(' AND ')}`;

    let sql = `
      UPDATE ${this.getSqlTable(name)}
      SET ${updateColumns}
      ${sqlWhere}
    `;

    sql = this.trigger(Database.EVENT_DOCUMENTS_UPDATE, sql);

    try {
      const [result] = await this.pool.query<any>(sql, values);
      const affected = result.affectedRows;

      if (updates.getPermissions()) {
        for (const document of documents) {
          const currentPermissions = await this.getCurrentPermissions(name, document.getId());
          const { additions, removals } = this.getPermissionChanges(currentPermissions, updates.getPermissions());

          if (Object.values(removals).some((perms: any) => perms.length > 0)) {
            await this.removePermissions(name, document.getId(), removals);
          }

          if (Object.values(additions).some((perms: any) => perms.length > 0)) {
            await this.addPermissions(name, document.getId(), additions);
          }
        }
      }

      return affected;
    } catch (e) {
      this.logger.error(e);
      throw new DatabaseError("Documents Update Failed.");
    }
  }

  /**
  * Retrieves the current permissions for a given document.
  *
  * @param name - The name of the entity for which permissions are being retrieved.
  * @param documentId - The ID of the document for which permissions are being retrieved.
  * @returns A promise that resolves to an object where the keys are permission types and the values are arrays of permissions.
  *
  * @remarks
  * This method constructs an SQL query to fetch permissions from a database table. If shared tables are used, additional SQL for tenant filtering is appended.
  */
  private async getCurrentPermissions(name: string, documentId: string) {
    let sql = `SELECT _type, _permission FROM ${this.getSqlTable(name + '_perms')} WHERE _document = ?`;
    if (this.sharedTables) {
      sql += this.getTenantSql();
    }

    const [permissions] = await this.pool.query<any>(sql, [documentId, this.tenantId]);
    return permissions.reduce((acc: any, { _type, _permission }: any) => {
      acc[_type] = acc[_type] || [];
      acc[_type].push(_permission);
      return acc;
    }, {});
  }

  /**
   * Computes the changes in permissions by comparing the current permissions with the new permissions.
   *
   * @param currentPermissions - An object representing the current permissions, where keys are permission types and values are arrays of resources.
   * @param newPermissions - An array of strings representing the new permissions in the format `action("resource")`.
   * @returns An object containing two properties:
   *   - `additions`: An object where keys are permission types and values are arrays of resources that need to be added.
   *   - `removals`: An object where keys are permission types and values are arrays of resources that need to be removed.
   */
  private getPermissionChanges(currentPermissions: any, newPermissions: any) {
    const additions: any = {};
    const removals: any = {};

    // Initialize additions and removals with empty arrays for each permission type
    for (const type of Database.PERMISSIONS) {
      additions[type] = [];
      removals[type] = [];
    }

    // Process new permissions
    for (const perm of newPermissions) {
      const [action, resource] = perm.match(/(\w+)\("([^"]+)"\)/).slice(1);
      if (!currentPermissions[action]?.includes(resource)) {
        additions[action].push(resource);
      }
    }

    // Process current permissions
    for (const type in currentPermissions) {
      for (const perm of currentPermissions[type]) {
        if (!newPermissions.includes(`${type}("${perm}")`)) {
          removals[type].push(perm);
        }
      }
    }

    return { additions, removals };
  }

  /**
    * Removes permissions from a specified document in the database.
    *
    * @param name - The name of the table (without the '_perms' suffix) from which permissions will be removed.
    * @param documentId - The ID of the document whose permissions are to be removed.
    * @param removals - An object where keys are permission types and values are arrays of permissions to be removed.
    * @returns A promise that resolves when the permissions have been removed.
    *
    * @remarkspublic async 
    * This method constructs a SQL DELETE statement to remove permissions from the specified document.
    * If `sharedTables` is enabled, it will also consider tenant-specific conditions.
    * The method triggers the `Database.EVENT_PERMISSIONS_DELETE` event before executing the query.
    */
  private async removePermissions(name: string, documentId: string, removals: any) {
    let sql = `DELETE FROM ${this.getSqlTable(name + '_perms')} WHERE _document = ?`;
    if (this.sharedTables) {
      sql += ' AND (_tenant = ? OR _tenant IS NULL)';
    }

    const conditions = [];
    const values = [documentId, this.tenantId];

    for (const type in removals) {
      const perms = removals[type].map((perm: string) => {
        values.push(type, perm);
        return `(_type = ? AND _permission = ?)`;
      }).join(' OR ');

      if (perms) conditions.push(`(${perms})`);
    }

    if (conditions.length > 0) {
      sql += ` AND (${conditions.join(' OR ')})`;
    }

    sql = this.trigger(Database.EVENT_PERMISSIONS_DELETE, sql);

    await this.pool.query<any>(sql, values);
  }

  /**
   * Adds permissions to a specified document in the database.
   *
   * @param name - The name of the collection or table to which permissions are being added.
   * @param documentId - The ID of the document to which permissions are being added.
   * @param additions - An object containing the types of permissions and their corresponding values to be added.
   * 
   * @remarks
   * This method constructs an SQL query to insert multiple permission entries into the database.
   * It uses placeholders for the values to prevent SQL injection and triggers an event before executing the query.
   * 
   * @returns A promise that resolves when the permissions have been successfully added to the database.
   */
  private async addPermissions(name: string, documentId: string, additions: any) {
    const values: any[] = [];
    const placeholders: string[] = [];

    for (const type in additions) {
      additions[type].forEach((perm: string) => {
        const columnValues: any[] = [documentId, type, perm];
        const placeholder = ['?', '?', '?'];

        if (this.sharedTables) {
          columnValues.push(this.getTenantId());
          placeholder.push('?');
        }

        values.push(...columnValues);
        placeholders.push(`(${placeholder.join(', ')})`);
      });
    }

    let sql = `INSERT INTO ${this.getSqlTable(name + '_perms')} (_document, _type, _permission${this.sharedTables ? ', _tenant' : ''}) VALUES ${placeholders.join(', ')}`;
    sql = this.trigger(Database.EVENT_PERMISSIONS_CREATE, sql);
    await this.pool.query<any>(sql, values);
  }

  /**
   * Increases the value of a specified attribute in a document within a collection.
   *
   * @param {string} collection - The name of the collection containing the document.
   * @param {string} id - The unique identifier of the document to update.
   * @param {string} attribute - The attribute whose value is to be increased.
   * @param {number} value - The amount by which to increase the attribute's value.
   * @param {string} updatedAt - The timestamp indicating when the update is made.
   * @param {number} [min] - Optional minimum value condition for the attribute.
   * @param {number} [max] - Optional maximum value condition for the attribute.
   * @returns {Promise<boolean>} - A promise that resolves to `true` if the update was successful, otherwise `false`.
   * @throws {DatabaseError} - Throws an error if the update operation fails.
   */
  public async increaseDocumentAttribute(
    collection: string,
    id: string,
    attribute: string,
    value: number,
    updatedAt: string,
    min?: number,
    max?: number
  ): Promise<boolean> {
    const name = this.filter(collection);
    const filteredAttribute = this.filter(attribute);
    const values: any[] = [];

    let sql = `
          UPDATE ${this.getSqlTable(name)}
          SET \`${filteredAttribute}\` = \`${filteredAttribute}\` + ?,
          _updatedAt = ?
          WHERE _uid = ?
      `;

    // Add values in order
    values.push(value);
    values.push(updatedAt);
    values.push(id);

    if (this.sharedTables) {
      sql += ' AND (_tenant = ? OR _tenant IS NULL)';
      values.push(this.tenantId);
    }

    // Add min/max conditions if provided
    if (max !== undefined) {
      sql += ` AND \`${filteredAttribute}\` <= ?`;
      values.push(max);
    }
    if (min !== undefined) {
      sql += ` AND \`${filteredAttribute}\` >= ?`;
      values.push(min);
    }

    sql = this.trigger(Database.EVENT_DOCUMENT_UPDATE, sql);

    try {
      const [result] = await this.pool.query<any>(sql, values);
      return result.affectedRows > 0;
    } catch (e) {
      this.logger.error(e);
      throw new DatabaseError("Failed to update attribute");
    }
  }

  /**
   * Delete a document from the specified collection.
   * 
   * @param collection - The name of the collection.
   * @param uid - The unique identifier of the document.
   * @returns A promise that resolves to a boolean indicating whether the document was successfully deleted.
   * @throws {DatabaseError} If the document deletion fails.
   */
  public async deleteDocument(collection: string, uid: string): Promise<boolean> {
    const name = this.filter(collection);
    const values: any[] = [uid];

    let sql = `DELETE FROM ${this.getSqlTable(name)} WHERE _uid = ?`;

    if (this.sharedTables) {
      sql += ' AND (_tenant = ? OR _tenant IS NULL)';
      values.push(this.tenantId);
    }

    sql = this.trigger(Database.EVENT_DOCUMENT_DELETE, sql);

    try {
      // Delete document
      const [result] = await this.pool.query<any>(sql, values);
      const deleted = result.affectedRows > 0;

      // Delete associated permissions
      let permSql = `DELETE FROM ${this.getSqlTable(name + '_perms')} WHERE _document = ?`;

      if (this.sharedTables) {
        permSql += ' AND (_tenant = ? OR _tenant IS NULL)';
      }

      permSql = this.trigger(Database.EVENT_PERMISSIONS_DELETE, permSql);
      await this.pool.query(permSql, values);

      return deleted;
    } catch (e) {
      this.logger.error(e);
      throw new DatabaseError("Document Deletion Failed");
    }
  }

  /**
   * Delete multiple documents from the specified collection.
   * 
   * @param collection - The name of the collection.
   * @param ids - An array of unique identifiers of the documents to be deleted.
   * @returns A promise that resolves to the number of documents successfully deleted.
   * @throws {DatabaseError} If the document deletion fails.
   */
  public async deleteDocuments(collection: string, ids: string[]): Promise<number> {
    const name = this.filter(collection);
    const values: any[] = ids;

    let sql = `DELETE FROM ${this.getSqlTable(name)} WHERE _uid IN (${ids.map(() => '?').join(', ')})`;

    if (this.sharedTables) {
      sql += ' AND (_tenant = ? OR _tenant IS NULL)';
      values.push(this.tenantId);
    }

    sql = this.trigger(Database.EVENT_DOCUMENTS_DELETE, sql);

    try {
      // Delete documents
      const [result] = await this.pool.query<any>(sql, values);
      const deletedCount = result.affectedRows;

      // Delete associated permissions
      let permSql = `DELETE FROM ${this.getSqlTable(name + '_perms')} WHERE _document IN (${ids.map(() => '?').join(', ')})`;

      if (this.sharedTables) {
        permSql += ' AND (_tenant = ? OR _tenant IS NULL)';
      }

      permSql = this.trigger(Database.EVENT_PERMISSIONS_DELETE, permSql);
      await this.pool.query(permSql, values);

      return deletedCount;
    } catch (e) {
      this.logger.error(e);
      throw new DatabaseError("Documents Deletion Failed");
    }
  }

  /**
   * Find documents in the specified collection based on the provided queries.
   * 
   * @param collection - The name of the collection.
   * @param queries - An array of query objects to filter the documents.
   * @param limit - The maximum number of documents to return.
   * @param offset - The number of documents to skip.
   * @param orderAttributes - An array of attributes to order the results by.
   * @param orderTypes - An array of order types corresponding to the order attributes.
   * @param cursor - An array representing the cursor for pagination.
   * @param cursorDirection - The direction of the cursor for pagination.
   * @param forPermission - The permission type to filter the documents by.
   * @returns A promise that resolves to an array of documents.
   * @throws {DatabaseError} If the document retrieval fails.
   */
  public async find(
    collection: string,
    queries: Query[] = [],
    limit: number = 25,
    offset: number | null = 0,
    orderAttributes: string[] = [],
    orderTypes: string[] = [],
    cursor: any = {},
    cursorDirection: string = Database.CURSOR_AFTER,
    forPermission: string = Database.PERMISSION_READ
  ): Promise<Document[]> {
    const name = this.filter(collection);
    const roles = Authorization.getRoles();
    const where: string[] = [];
    const orders: string[] = [];
    const params: any = [];

    // Map order attributes
    orderAttributes = orderAttributes.map(orderAttribute => {
      switch (orderAttribute) {
        case '$id': return '_uid';
        case '$internalId': return '_id';
        case '$tenant': return '_tenant';
        case '$createdAt': return '_createdAt';
        case '$updatedAt': return '_updatedAt';
        default: return orderAttribute;
      }
    });

    let hasIdAttribute = false;
    orderAttributes.forEach((attribute, i) => {
      if (attribute === '_uid') hasIdAttribute = true;

      const filteredAttribute = this.filter(attribute);
      let orderType = this.filter(orderTypes[i] || Database.ORDER_ASC);

      // Handle cursor-based pagination
      if (i === 0 && cursor) {
        const cursorValue = cursor[attribute === '_uid' ? '$id' : attribute] || null;
        const internalId = cursor.$internalId;

        if (cursorValue === undefined || internalId === undefined) {
          return;
        }

        const orderMethod = orderType === Database.ORDER_DESC
          ? Query.TYPE_LESSER
          : Query.TYPE_GREATER;

        const orderMethodInternalId = cursorDirection === Database.CURSOR_BEFORE
          ? Query.TYPE_LESSER
          : Query.TYPE_GREATER;

        where.push(`(
                table_main.\`${filteredAttribute}\` ${this.getSQLOperator(orderMethod)} ?
                OR (
                    table_main.\`${filteredAttribute}\` = ?
                    AND table_main._id ${this.getSQLOperator(orderMethodInternalId)} ?
                )
            )`);

        params.push(cursorValue);
        params.push(cursorValue);
        params.push(internalId);
        // params.cursorValue = cursorValue;
        // params.internalId = internalId;
      } else if (cursorDirection === Database.CURSOR_BEFORE) {
        orderType = orderType === Database.ORDER_ASC ? Database.ORDER_DESC : Database.ORDER_ASC;
      }

      orders.push(`\`${filteredAttribute}\` ${orderType}`);
    });

    // Add default order by _id if not present
    if (!hasIdAttribute) {
      orders.push(`table_main._id ${cursorDirection === Database.CURSOR_AFTER ? 'ASC' : 'DESC'}`);
    }

    // Construct WHERE conditions from queries
    const conditions = this.getSQLConditions(queries);
    if (conditions) {
      where.push(conditions);
      let cParams: any = [];
      for (const query of queries) {
        this.bindConditionValue(cParams, query);
      }
      params.push(...cParams);
    }

    // Add permissions and tenant-based conditions
    if (Authorization.status) {
      where.push(this.getSQLPermissionsCondition(name, roles, forPermission));
      if (this.sharedTables) {
        // params.tenant = this.tenantId;
        params.push(this.tenantId);
      }
    }

    if (this.sharedTables) {
      where.push("(table_main._tenant = ? OR table_main._tenant IS NULL)");
      // params.tenant = this.tenantId;
      params.push(this.tenantId);
    }

    // Build final SQL query
    const sqlWhere = where.length ? `WHERE ${where.join(' AND ')}` : '';
    const sqlOrder = `ORDER BY ${orders.join(', ')}`;
    const sqlLimit = limit !== null && limit !== undefined ? `LIMIT ?` : '';
    const sqlOffset = offset !== null && limit !== undefined ? `OFFSET ?` : '';

    const selections = this.getAttributeSelections(queries);
    const sql = `
        SELECT ${this.getAttributeProjection(selections, 'table_main')}
        FROM ${this.getSqlTable(name)} AS table_main
        ${sqlWhere}
        ${sqlOrder}
        ${sqlLimit}
        ${sqlOffset};
    `;

    if (limit !== undefined && limit !== null) params.push(limit);
    if (offset !== null && offset !== undefined) params.push(offset);

    try {
      const [results] = await this.pool.execute<any[]>(sql, params);

      let res = results.map(result => this.objectToDocument(result));

      if (cursorDirection === Database.CURSOR_BEFORE) {
        res = res.reverse();
      }

      return res;
    } catch (error) {
      this.logger.error(error);
      throw this.processException(error);
    }
  }


  /**
   * Count documents in the specified collection based on the provided queries.
   * 
   * @param collection - The name of the collection.
   * @param queries - An array of query objects to filter the documents.
   * @param max - The maximum number of documents to count.
   * @returns A promise that resolves to the count of documents.
   * @throws {DatabaseError} If the document count fails.
   */
  public async count(
    collection: string,
    queries: Query[] = [],
    max: number | null = null
  ): Promise<number> {
    const name = this.filter(collection);
    const roles = Authorization.getRoles();
    const where: string[] = [];
    const params: any = [];

    // Construct WHERE conditions from queries
    const conditions = this.getSQLConditions(queries);
    if (conditions) {
      where.push(conditions);
      let cParams: any = [];
      for (const query of queries) {
        this.bindConditionValue(cParams, query);
      }
      params.push(...cParams);
    }

    // Add permissions and tenant-based conditions
    if (Authorization.status) {
      where.push(this.getSQLPermissionsCondition(name, roles));
      if (this.sharedTables) {
        params.push(this.tenantId);
      }
    }

    if (this.sharedTables) {
      where.push("(table_main._tenant = ? OR table_main._tenant IS NULL)");
      params.push(this.tenantId);
    }

    // Build final SQL query
    const sqlWhere = where.length ? `WHERE ${where.join(' AND ')}` : '';
    const sqlLimit = max !== null ? `LIMIT ?` : '';

    const sql = `
      SELECT COUNT(1) as sum FROM (
        SELECT 1
        FROM ${this.getSqlTable(name)} table_main
        ${sqlWhere}
        ${sqlLimit}
      ) table_count;
    `;

    if (max !== null) params.push(max);

    try {
      const [results] = await this.pool.execute<any[]>(sql, params);
      const result = results[0];
      return result.sum ?? 0;
    } catch (error) {
      this.logger.error(error);
      throw new DatabaseError("Document Count Failed.");
    }
  }

  /**
   * Sum the values of a specified attribute in the collection based on the provided queries.
   * 
   * @param collection - The name of the collection.
   * @param attribute - The attribute to sum.
   * @param queries - An array of query objects to filter the documents.
   * @param max - The maximum number of documents to sum.
   * @returns A promise that resolves to the sum of the attribute values.
   * @throws {DatabaseError} If the sum operation fails.
   */
  public async sum(
    collection: string,
    attribute: string,
    queries: Query[] = [],
    max: number | null = null
  ): Promise<number> {
    const name = this.filter(collection);
    const roles = Authorization.getRoles();
    const where: string[] = [];
    const params: any = [];

    // Construct WHERE conditions from queries
    const conditions = this.getSQLConditions(queries);
    if (conditions) {
      where.push(conditions);
      let cParams: any = [];
      for (const query of queries) {
        this.bindConditionValue(cParams, query);
      }
      params.push(...cParams);
    }

    // Add permissions and tenant-based conditions
    if (Authorization.status) {
      where.push(this.getSQLPermissionsCondition(name, roles));
      if (this.sharedTables) {
        params.push(this.tenantId);
      }
    }

    if (this.sharedTables) {
      where.push("(table_main._tenant = ? OR table_main._tenant IS NULL)");
      params.push(this.tenantId);
    }

    // Build final SQL query
    const sqlWhere = where.length ? `WHERE ${where.join(' AND ')}` : '';
    const sqlLimit = max !== null ? `LIMIT ?` : '';

    const sql = `
      SELECT SUM(${attribute}) as sum FROM (
        SELECT ${attribute}
        FROM ${this.getSqlTable(name)} table_main
        ${sqlWhere}
        ${sqlLimit}
      ) table_count;
    `;

    if (max !== null) params.push(max);

    try {
      const [results] = await this.pool.execute<any[]>(sql, params);
      const result = results[0];
      return result.sum ?? 0;
    } catch (error) {
      this.logger.error(error);
      throw new DatabaseError("Sum Operation Failed.");
    }
  }


  /**
   * Get a document from the specified collection.
   * 
   * @param collection - The name of the collection.
   * @param uid - The unique identifier of the document.
   * @returns A promise that resolves to the document.
   * @throws {DatabaseError} If the document retrieval fails.
   */
  public async getDocument(collection: string, uid: string): Promise<Document> {
    const name = this.filter(collection);
    const sql = `SELECT * FROM ${this.getSqlTable(name)} WHERE _uid = ?`;
    try {
      const [result] = await this.pool.query<any>(sql, [uid]);
      return this.objectToDocument(result[0]);
    } catch (e) {
      this.logger.error(e);
      throw this.processException(e);
    }
  }

  /**
   * Get all documents from the specified collection.
   * 
   * @param collection - The name of the collection.
   * @returns A promise that resolves to an array of documents.
   * @throws {DatabaseError} If the document retrieval fails.
   */
  public async getDocuments(collection: string): Promise<Document[]> {
    const name = this.filter(collection);
    const sql = `SELECT * FROM ${this.getSqlTable(name)}`;
    try {
      const [result] = await this.pool.query<any>(sql);
      return result.map((doc: any) => this.objectToDocument(doc));
    } catch (e) {
      this.logger.error(e);
      throw this.processException(e)
    }
  }

  public async getSizeOfCollectionOnDisk(collection: string): Promise<number> {
    const filteredCollection = this.filter(collection);
    let prefixPart = this.perfix ? `${this.perfix}_` : '';
    const name = `${this.getDatabase()}/${prefixPart}${filteredCollection}`;
    const permissions = `${this.getDatabase()}/${prefixPart}${filteredCollection}_perms`;

    const sql = `
      SELECT SUM(FS_BLOCK_SIZE + ALLOCATED_SIZE)  
      FROM INFORMATION_SCHEMA.INNODB_SYS_TABLESPACES
      WHERE NAME IN (?, ?)
    `;

    try {
      const [result] = await this.pool.query<any>(sql, [name, permissions]);
      return Number(result[0]['SUM(FS_BLOCK_SIZE + ALLOCATED_SIZE)']) || 0;
    } catch (e) {
      this.logger.error(e);
      throw this.processException(e);
    }
  }

  public async getSizeOfCollection(collection: string): Promise<number> {
    const filteredCollection = this.filter(collection);
    let prefixPart = this.perfix ? `${this.perfix}_` : '';
    const tableName = `${prefixPart}${filteredCollection}`;
    const permissionsTable = `${prefixPart}${tableName}_perms`;
    const database = this.getDatabase();

    const sql = `
      SELECT SUM(data_length + index_length)
      FROM INFORMATION_SCHEMA.TABLES
      WHERE table_schema = ?
      AND table_name IN (?, ?)
    `;

    try {
      const [result] = await this.pool.query<any>(sql, [database, tableName, permissionsTable]);
      return Number(result[0]['SUM(data_length + index_length)']) || 0;
    } catch (e) {
      this.logger.error(e);
      throw this.processException(e);
    }
  }

  /**
   * Converts a given object to a Document instance by mapping and renaming specific properties.
   * 
   * @param obj - The object to be converted.
   * @returns A new Document instance with the mapped properties.
   * 
   * The following properties are mapped:
   * - `$id` is set to the value of `_uid` or `null` if `_uid` is not present.
   * - `$internalId` is set to the value of `_id` or `null` if `_id` is not present.
   * - `$tenant` is set to the value of `_tenant` or `null` if `_tenant` is not present.
   * - `$createdAt` is set to the value of `_createdAt` or `null` if `_createdAt` is not present.
   * - `$updatedAt` is set to the value of `_updatedAt` or `null` if `_updatedAt` is not present.
   * - `$permissions` is set to the parsed JSON value of `_permissions` or an empty array if `_permissions` is not present.
   * 
   * The original properties (`_uid`, `_id`, `_tenant`, `_createdAt`, `_updatedAt`, `_permissions`) are deleted from the object.
   */
  objectToDocument(obj: any): Document {
    if (!obj) return new Document();
    obj.$id = obj?._uid || null; delete obj?._uid;
    obj.$internalId = obj?._id || null; delete obj?._id;
    obj.$tenant = obj?._tenant || null; delete obj?._tenant;
    obj.$createdAt = obj?._createdAt || null; delete obj?._createdAt;
    obj.$updatedAt = obj?._updatedAt || null; delete obj?._updatedAt;
    obj.$permissions = obj?._permissions ? JSON.parse(obj._permissions) : []; delete obj?._permissions;
    return new Document(obj);
  }


  /**
   * Get attribute projection for SQL queries.
   *
   * @param selections - The array of selected attributes.
   * @param prefix - The prefix to be added to the attributes.
   * @returns The SQL projection string.
   */
  protected getAttributeProjection(selections: string[], prefix: string = ''): string {
    if (selections.length === 0 || selections.includes('*')) {
      return prefix ? `\`${prefix}\`.*` : '*';
    }

    // Remove $id, $permissions, and $collection if present since they are always selected by default
    selections = selections.filter(selection => !['$id', '$permissions', '$collection'].includes(selection));

    selections.push('_uid', '_permissions');

    if (selections.includes('$internalId')) {
      selections.push('_id');
      selections = selections.filter(selection => selection !== '$internalId');
    }
    if (selections.includes('$createdAt')) {
      selections.push('_createdAt');
      selections = selections.filter(selection => selection !== '$createdAt');
    }
    if (selections.includes('$updatedAt')) {
      selections.push('_updatedAt');
      selections = selections.filter(selection => selection !== '$updatedAt');
    }

    if (prefix) {
      selections = selections.map(selection => `\`${prefix}\`.\`${this.filter(selection)}\``);
    } else {
      selections = selections.map(selection => `\`${this.filter(selection)}\``);
    }

    return selections.join(', ');
  }

  /**
   * Process MySQL exceptions and map them to custom exceptions.
   *
   * @param e - The MySQL error object.
   * @returns The mapped custom exception.
   */
  protected processException(e: any): DatabaseError {
    // Timeout
    if (e.code === 'PROTOCOL_SEQUENCE_TIMEOUT') {
      return new TimeoutException('Query timed out', e.code, e);
    }

    // Duplicate table
    if (e.code === 'ER_TABLE_EXISTS_ERROR') {
      return new DuplicateException('Collection already exists', e.code, e);
    }

    // Duplicate column
    if (e.code === 'ER_DUP_FIELDNAME') {
      return new DuplicateException('Attribute already exists', e.code, e);
    }

    // Duplicate index
    if (e.code === 'ER_DUP_KEYNAME') {
      return new DuplicateException('Index already exists', e.code, e);
    }

    // Duplicate row
    if (e.code === 'ER_DUP_ENTRY') {
      return new DuplicateException('Document already exists', e.code, e);
    }

    // Data is too big for column resize
    if (e.code === 'ER_DATA_TOO_LONG' || e.code === 'ER_WARN_DATA_OUT_OF_RANGE') {
      return new TruncateException('Resize would result in data truncation', e.code, e);
    }

    // Unknown database
    if (e.code === 'ER_BAD_DB_ERROR') {
      return new NotFoundException('Database not found', e.code, e);
    }

    return e;
  }

  public getSupportForIndex(): boolean {
    return true;
  }

  public getSupportForUniqueIndex(): boolean {
    return true;
  }

  public getSupportForFulltextIndex(): boolean {
    return true;
  }

  public getSupportForFulltextWildcardIndex(): boolean {
    return true;
  }

  /**
   * Get the minimum DateTime value supported by MariaDB.
   *
   * @returns {Date} The minimum DateTime value as a Date object.
   */
  public getMinDateTime(): Date {
    return new Date('1000-01-01T00:00:00Z');
  }

  /**
   * Get the maximum DateTime value supported by MariaDB.
   *
   * @returns {Date} The maximum DateTime value as a Date object.
   */
  public getMaxDateTime(): Date {
    return new Date('9999-12-31T23:59:59Z');
  }

}