import { Database } from "../core/database";
import { Document } from "../core/Document";
import { Attribute } from "../core/types/attribute";
import { Index, IndexType } from "../core/types/indexes";
import { InitializeError } from "../errors/adapter";
import { DatabaseError } from "../errors/base";
import { Adapter, DatabaseAdapter } from "./base";
import * as mysql2 from 'mysql2/promise';

interface MariaDBOptions {
  connection: mysql2.PoolOptions;
}

/**
 * MariaDB adapter class
 */
export class MariaDB extends DatabaseAdapter implements Adapter {
  /**
   * MariaDB pool / connection
   */
  pool: mysql2.Pool;

  /**
   * MariaDB adapter library
   */
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
  // private getTenantSql(tenantId?: string) {
  //   tenantId = tenantId ? tenantId : this.tenantId;

  //   if (this.sharedTables && !this.tenantId) throw new DatabaseError('Tenant ID is required for shared tables')

  //   if (this.tenantId && this.sharedTables) {
  //     return ` AND tenant_id = '${tenantId}'`
  //   }
  //   return '';
  // }

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
  public async createCollection(name: string, attributes: Attribute[], indexes: Index[], ifExists: boolean = false): Promise<boolean> {

    name = this.getSqlTable(name);

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
   * Drops a collection from the database if it exists.
   *
   * @param name - The name of the collection to drop.
   * @param ifExists - A boolean flag indicating whether to include the "IF EXISTS" clause in the SQL statement. Defaults to false.
   * @returns A promise that resolves to a boolean indicating whether the collection was successfully dropped.
   * @throws {DatabaseError} If the collection drop operation fails.
   */
  public async dropCollection(name: string, ifExists: boolean = false): Promise<boolean> {
    name = this.getSqlTable(name);

    ifExists = ifExists ? 'IF EXISTS' : '' as any;

    let sql = `DROP TABLE ${ifExists} ${name}, ${name}_perms`;

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
        // TODO: ----
        // const collectionDoc = await this.getDocument(Database.METADATA, collection);
        // const relatedCollectionDoc = await this.getDocument(Database.METADATA, relatedCollection);

        // const junction = this.getSqlTable(`_${collectionDoc.getInternalId()}_${relatedCollectionDoc.getInternalId()}`);

        // if (newKey) {
        //   sql = `ALTER TABLE ${junction} RENAME COLUMN \`${key}\` TO \`${newKey}\`;`;
        // }
        // if (twoWay && newTwoWayKey) {
        //   sql += `ALTER TABLE ${junction} RENAME COLUMN \`${twoWayKey}\` TO \`${newTwoWayKey}\`;`;
        // }
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

    let permissions = document.getAttribute("permissions")

    document.removeAttribute("permissions")

    let columns: string[] = [];
    let values: any[] = [];
    let placeholders: string[] = [];

    // Process attributes
    Object.entries(document.getAttributes()).forEach(([attribute, value], index) => {
      const column = this.filter(attribute);
      columns.push(`\`${column}\``);
      placeholders.push('?');
      values.push(Array.isArray(value) || typeof value === 'object' ?
        JSON.stringify(value) :
        // typeof value === 'boolean' ? Number(value) : 
        value
      );
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

    const sql = `
    INSERT INTO ${this.getSqlTable(name)} 
    (${columns.join(', ')})
    VALUES (${placeholders.join(', ')})
    `;

    document.setAttribute("permissions", permissions)

    try {
      const [result] = await this.pool.query<any>(sql, values);
      this.logger.debug(result);

      // Handle permissions if any
      if (document.getPermissions()) {
        const permValues: any[] = [];
        const permPlaceholders: string[] = [];

        ['read', 'write', 'update', 'delete'].forEach(type => {
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
          const permSql = `
                INSERT INTO ${this.getSqlTable(name + '_perms')}
                (_type, _permission, _document${this.sharedTables ? ', _tenant' : ''})
                VALUES ${permPlaceholders.join(', ')}
            `;
          await this.pool.query(permSql, permValues);
        }
      }

      this.logger.debug(result);
      return result;
    } catch (e) {
      this.logger.error(e);
      throw new DatabaseError("Document Creation Failed");
    }
  }

  /**
   * Update a document in the specified collection.
   * 
   * @param collection - The name of the collection.
   * @param document - The document to be updated.
   * @returns A promise that resolves to a boolean indicating whether the document was successfully updated.
   * @throws {DatabaseError} If the document update fails.
   */
  public async updateDocument<T extends Document>(collection: string, document: Document): Promise<T> {
    const name = this.filter(collection);
    const sql = `UPDATE ${this.getSqlTable(name)} SET ? WHERE _uid = ?`;
    try {
      const [result] = await this.pool.query<any>(sql, [document, document.getId()]);
      this.logger.debug(result);
      return result
    } catch (e) {
      this.logger.error(e);
      throw new DatabaseError("Document Update Failed.");
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
    const sql = `DELETE FROM ${this.getSqlTable(name)} WHERE _uid = ?`;
    try {
      const [result] = await this.pool.query<any>(sql, [uid]);
      this.logger.debug(result);
      return true
    } catch (e) {
      this.logger.error(e);
      throw new DatabaseError("Document Deletion Failed.");
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
  public async getDocument<T extends Document>(collection: string, uid: string): Promise<T> {
    const name = this.filter(collection);
    const sql = `SELECT * FROM ${this.getSqlTable(name)} WHERE _uid = ?`;
    try {
      const [result] = await this.pool.query<any>(sql, [uid]);
      this.logger.debug(result);
      return result
    } catch (e) {
      this.logger.error(e);
      throw new DatabaseError("Document Retrieval Failed.");
    }
  }

  /**
   * Get all documents from the specified collection.
   * 
   * @param collection - The name of the collection.
   * @returns A promise that resolves to an array of documents.
   * @throws {DatabaseError} If the document retrieval fails.
   */
  public async getDocuments<T extends Document>(collection: string): Promise<T[]> {
    const name = this.filter(collection);
    const sql = `SELECT * FROM ${this.getSqlTable(name)}`;
    try {
      const [result] = await this.pool.query<any>(sql);
      this.logger.debug(result);
      return result
    } catch (e) {
      this.logger.error(e);
      throw new DatabaseError("Document Retrieval Failed.");
    }
  }

}