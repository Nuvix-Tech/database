import { Database } from "../core/database";
import { Query } from "../core/query";
import { DatabaseError } from "../errors/base";
import { DatabaseAdapter } from "./base";


export abstract class Sql extends DatabaseAdapter {

  /**
   * Get SQL condition
   *
   * @param query - Query to get condition for
   */
  protected getSQLOperator(method: string): string {
    switch (method) {
      case Query.TYPE_EQUAL:
        return '=';
      case Query.TYPE_NOT_EQUAL:
        return '!=';
      case Query.TYPE_LESSER:
        return '<';
      case Query.TYPE_LESSER_EQUAL:
        return '<=';
      case Query.TYPE_GREATER:
        return '>';
      case Query.TYPE_GREATER_EQUAL:
        return '>=';
      case Query.TYPE_IS_NULL:
        return 'IS NULL';
      case Query.TYPE_IS_NOT_NULL:
        return 'IS NOT NULL';
      case Query.TYPE_STARTS_WITH:
      case Query.TYPE_ENDS_WITH:
      case Query.TYPE_CONTAINS:
        return this.getLikeOperator();
      default:
        throw new DatabaseError('Unknown method: ' + method);
    }
  }

  public escapeWildcards(value: string): string {
    const wildcards = ['%', '_', '[', ']', '^', '-', '.', '*', '+', '?', '(', ')', '{', '}', '|'];
    for (const w of wildcards) {
      value = value.replace(new RegExp(`\\${w}`, 'g'), `\\${w}`);
    }
    return value;
  }

  protected getSQLIndexType(type: string): string {
    switch (type) {
      case Database.INDEX_KEY:
        return 'INDEX';
      case Database.INDEX_UNIQUE:
        return 'UNIQUE INDEX';
      case Database.INDEX_FULLTEXT:
        return 'FULLTEXT INDEX';
      default:
        throw new DatabaseError('Unknown index type: ' + type);
    }
  }

  protected getSQLPermissionsCondition(
    collection: string,
    roles: string[],
    type: string = Database.PERMISSION_READ,
    namedPlaceholders: boolean = false
  ): string {
    if (!Database.PERMISSIONS.includes(type)) {
      throw new DatabaseError('Unknown permission type: ' + type);
    }
    const quotedRoles = roles.map(r => `'${r}'`).join(', ');
    let tenantQuery = '';
    if (this.sharedTables) {
      tenantQuery = `AND (_tenant = ${namedPlaceholders ? ':tenant' : '?'} OR _tenant IS NULL)`;
    }
    return `table_main._uid IN (
      SELECT _document
      FROM ${this.getSQLTable(collection + '_perms')}
      WHERE _permission IN (${quotedRoles})
        AND _type = '${type}'
        ${tenantQuery}
    )`;
  }

  protected getSQLTable(name: string): string {
    const prefixPart = this.perfix ? `${this.perfix}_` : '';
    return `\`${this.getDatabase()}\`.\`${prefixPart}${this.filter(name)}\``;
  }

  protected getSqlTable(name: string) {
    return this.getSQLTable(name);
  }

  public getSQLConditions(queries: Query[] = [], separator: string = 'AND'): string {
    const parts: string[] = [];
    for (const q of queries) {
      if (q.getMethod() === Query.TYPE_SELECT) {
        continue;
      }
      if (q.isNested()) {
        const nested = this.getSQLConditions(q.getValues(), q.getMethod());
        parts.push(nested);
      } else {
        const condition = this.getSQLCondition(q);
        parts.push(condition);
      }
    }
    if (!parts.length) {
      return '';
    }
    return '(' + parts.join(' ' + separator + ' ') + ')';
  }

  public getLikeOperator(): string {
    return 'LIKE';
  }

  protected getSQLPlaceholder(query: Query): string {
    const values = query.getValues();
    if (!values || values.length === 0) {
      throw new DatabaseError(`Invalid query values for attribute: ${query.getAttribute()}`);
    }
    const json = JSON.stringify([query.getAttribute(), query.getMethod(), values]);
    return require('crypto').createHash('md5').update(json).digest('hex');
  }

  protected getAttributeSelections(queries: Array<Query>): Array<string> {
    const selections: Array<string> = [];
    for (const query of queries) {
      if (query.getMethod() === Query.TYPE_SELECT) {
        selections.push(...query.getValues());
      }
    }
    return selections;
  }


  public abstract getSQLCondition(query: Query): string;


  protected bindConditionValue(
    params: any[], //Record<string, any>,
    query: Query
  ): void {
    if (query.getMethod() === Query.TYPE_SELECT) {
      // Skip binding for SELECT queries
      return;
    }

    if (query.isNested()) {
      // Recursively bind values for nested queries
      for (const nestedQuery of query.getValues()) {
        this.bindConditionValue(params, nestedQuery);
      }
      return;
    }

    if (this.supportsJSONOverlaps() && query.onArray() && query.getMethod() === Query.TYPE_CONTAINS) {
      // Handle JSON overlaps with arrays
      // const placeholder = `${this.getSQLPlaceholder(query)}_0`;
      params.push(JSON.stringify(query.getValues()));
      // params[placeholder] = JSON.stringify(query.getValues());
      return;
    }

    query.getValues().forEach((value, key) => {
      // Transform the value based on the query type
      value = (() => {
        switch (query.getMethod()) {
          case Query.TYPE_STARTS_WITH:
            return this.escapeWildcards(value) + '%';
          case Query.TYPE_ENDS_WITH:
            return '%' + this.escapeWildcards(value);
          case Query.TYPE_SEARCH:
            return this.getFulltextValue(value);
          case Query.TYPE_CONTAINS:
            return query.onArray() ? JSON.stringify(value) : `%${this.escapeWildcards(value)}%`;
          default:
            return value;
        }
      })();

      // Add the transformed value to the params object with a unique key
      // const placeholder = `${this.getSQLPlaceholder(query)}_${key}`;
      params.push(value);
      // params[placeholder] = value;
    });
  }

  protected getFulltextValue(value: string): string {
    const starts = value.startsWith('"');
    const ends = value.endsWith('"');
    const specialChars = '@,+,-,*,),(,<,>,~,"';
    let replaced = value;
    for (const ch of specialChars.split(',')) {
      replaced = replaced.split(ch).join(' ');
    }
    replaced = replaced.replace(/\s+/g, ' ').trim();
    if (!replaced) return '';
    if (starts && ends) {
      replaced = `"${replaced}"`;
    } else {
      replaced += '*';
    }
    return replaced;
  }

  supportsJSONOverlaps(): boolean {
    return false;
  }

}