import { DatabaseException } from "@errors/base.js";
import { EventEmitter } from "stream";
import { IAdapter, IClient } from "./interface.js";
import { AttributeEnum, CursorEnum, EventsEnum, IndexEnum, OrderEnum, PermissionEnum } from "@core/enums.js";
import { Find, IncreaseDocumentAttribute } from "./types.js";
import { Doc } from "@core/doc.js";
import { Database, ProcessQuery } from "@core/database.js";
import { QueryBuilder } from "@utils/query-builder.js";
import { Query, QueryType } from "@core/query.js";
import { Entities, IEntity } from "types.js";
import { Logger } from "@utils/logger.js";
import { Authorization } from "@utils/authorization.js";
import { Collection } from "@validators/schema.js";

export abstract class BaseAdapter extends EventEmitter {
    public readonly type: string = 'base';
    protected _meta: Partial<Meta> = { schema: 'public' };
    protected abstract client: IClient;
    protected $logger = new Logger();

    protected $timeout: number = 0;

    readonly $limitForString: number = 10485760;
    readonly $limitForInt: bigint = 9223372036854775807n;
    readonly $limitForAttributes: number = 1600;
    readonly $limitForIndexes: number = 64;
    readonly $supportForSchemas: boolean = true;
    readonly $supportForIndex: boolean = true;
    readonly $supportForAttributes: boolean = true;
    readonly $supportForUniqueIndex: boolean = true;
    readonly $supportForFulltextIndex: boolean = true;
    readonly $supportForUpdateLock: boolean = true;
    readonly $supportForAttributeResizing: boolean = true;
    readonly $supportForBatchOperations: boolean = true;
    readonly $supportForGetConnectionId: boolean = false;
    readonly $supportForCacheSkipOnFailure: boolean = true;
    readonly $supportForHostname: boolean = true;
    readonly $documentSizeLimit: number = 16777216;
    readonly $supportForCasting: boolean = true;
    readonly $supportForNumericCasting: boolean = true;
       readonly $supportForQueryContains: boolean = true;
    readonly $supportForIndexArray: boolean = true;
    readonly $supportForCastIndexArray: boolean = true;
    readonly $supportForRelationships: boolean = true;
    readonly $supportForReconnection: boolean = true;
    readonly $supportForBatchCreateAttributes: boolean = true;
    readonly $maxVarcharLength: number = 10485760;
    readonly $maxIndexLength: number = 8191;
    readonly $supportForJSONOverlaps: boolean = true;

    protected transformations: Partial<Record<EventsEnum, Array<[string, (query: string) => string]>>> = {
        [EventsEnum.All]: [],
    }

    constructor(options: { type?: string } = {}) {
        super();
        if (options.type) {
            this.type = options.type;
        }
    }

    /**@deprecated temp */
    public get $database(): string {
        if (!this._meta.database) throw new DatabaseException('Database name is not defined in adapter metadata.');
        return this._meta.database;
    }

    public get $schema(): string {
        if (!this._meta.schema) throw new DatabaseException('Schema name is not defined in adapter metadata.');
        return this._meta.schema;
    }

    public get $sharedTables(): boolean {
        const sharedTables = this._meta.sharedTables;
        if (sharedTables && !this._meta.tenantId) {
            Logger.warn('Shared tables are enabled but tenantId is not defined in adapter metadata. This may lead to unexpected behavior.');
        }
        return !!sharedTables;
    }

    public get $tenantId(): number | undefined {
        return this._meta.tenantId;
    }

    public get $tenantPerDocument(): boolean {
        return !!this._meta.tenantPerDocument;
    }

    public get $namespace(): string {
        return this._meta.namespace ?? 'default';
    }

    public get $metadata() {
        return this._meta.metadata ?? {};
    }

    public get $client() {
        return this.client;
    }

    public setMeta(meta: Partial<Meta>) {
        if (this._meta.metadata) {
            this._meta.metadata = { ...this._meta.metadata, ...meta.metadata };
            let metaString: string = '';

            for (const [key, value] of Object.entries(this._meta.metadata)) {
                metaString += `/* ${key}: ${value} */\n`;
            }

            this.before(EventsEnum.All, 'metadata', (query: string) => {
                return metaString + query;
            });
        }
        this._meta = { ...this._meta, ...meta };
        return this;
    }

    public before(event: EventsEnum, name: string, callback?: (query: string) => string): void {
        if (!this.transformations[event]) {
            this.transformations[event] = [];
        }
        if (callback) {
            this.transformations[event].push([name, callback]);
        } else {
            const index = this.transformations[event].findIndex(transformation => transformation[0] === name);
            if (index !== -1) {
                this.transformations[event].splice(index, 1);
            }
        }
    }

    public trigger(event: EventsEnum, query: string): string {
        for (const transformation of this.transformations[EventsEnum.All] || []) {
            query = transformation[1](query);
        }
        for (const transformation of this.transformations[event] || []) {
            query = transformation[1](query);
        }
        return query;
    }

    public sanitize(value: string): string {
        if (value === null || value === undefined) {
            throw new DatabaseException(
                "Failed to sanitize key: value is null or undefined",
            );
        }

        const sanitized = value.replace(/[^A-Za-z0-9_\-]/g, "");
        if (sanitized === "") {
            throw new DatabaseException(
                "Failed to sanitize key: filtered value is empty",
            );
        }

        return sanitized;
    }

    public async ping(): Promise<void> {
        return await this.client.ping();
    }

    protected get $(): IAdapter {
        return this as unknown as IAdapter;
    }

    async exists(name: string, collection?: string): Promise<boolean> {
        const values: string[] = [this.sanitize(name)];
        let sql: string;

        if (collection) {
            sql = `
                SELECT COUNT(*) as count
                FROM information_schema.tables
                WHERE table_schema = $1
                AND table_name = $2
            `;
            values.push(this.sanitize(collection));
        } else {
            sql = `
                SELECT COUNT(*) as count
                FROM information_schema.schemata
                WHERE schema_name = $1
            `;
        }

        const { rows } = await this.client.query<any>(sql, values);
        return rows[0].count > 0;
    }

    public async getDocument<C extends (string & keyof Entities)>(
        collection: C,
        id: string,
        queries?: ProcessQuery | null,
        forUpdate?: boolean
    ): Promise<Doc<Entities[C]>>;
    public async getDocument<C extends Record<string, any>>(
        collection: string,
        id: string,
        queries?: ProcessQuery | null,
        forUpdate?: boolean
    ): Promise<Doc<Partial<IEntity> & C>>;
    public async getDocument(
        collection: string,
        id: string,
        { queries, selections }: ProcessQuery,
        forUpdate: boolean = false
    ): Promise<Doc<Partial<IEntity> & Record<string, any>>> {
        if (!collection || !id) {
            throw new DatabaseException("Failed to get document: collection and id are required");
        }

        const table = this.getSQLTable(collection);
        const alias = Query.DEFAULT_ALIAS;
        const params: any[] = [id];

        let sql = `
            SELECT '${collection}' AS "$collection", ${this.getAttributeProjection(selections, alias)}
            FROM ${table} AS ${alias}
            WHERE ${this.quote(alias)}.${this.quote('_uid')} = ?
            ${this.getTenantQuery(collection, alias)}
        `;

        if (forUpdate && this.$supportForUpdateLock) {
            sql += ' FOR UPDATE';
        }

        if (this.$sharedTables) {
            params.push(this.$tenantId);
        }

        const { rows } = await this.client.query<any>(sql, params);

        let document = rows[0];

        return new Doc(document);
    }

    public async deleteDocument(
        collection: string,
        id: string
    ): Promise<boolean> {
        if (!collection || !id) {
            throw new DatabaseException("Failed to delete document: collection and id are required");
        }

        try {
            const table = this.getSQLTable(collection);
            const params: any[] = [id];

            let sql = `
                DELETE FROM ${table}
                WHERE ${this.$.quote('_uid')} = ?
                ${this.getTenantQuery(collection)}
            `;

            sql = this.trigger(EventsEnum.DocumentDelete, sql);

            if (this.$sharedTables) {
                params.push(this.$tenantId);
            }

            const { rows: result } = await this.client.query<any>(sql, params);

            // Delete permissions
            const permParams: any[] = [id];
            let permSql = `
                DELETE FROM ${this.getSQLTable(collection + '_perms')}
                WHERE ${this.$.quote('_document')} = ?
                ${this.getTenantQuery(collection)}
                RETURNING _id
            `;

            permSql = this.trigger(EventsEnum.PermissionsDelete, permSql);

            if (this.$sharedTables) {
                permParams.push(this.$tenantId);
            }

            await this.client.query(permSql, permParams);

            return result.length > 0;
        } catch (error) {
            this.processException(error, 'Failed to delete document');
        }
    }

    public async increaseDocumentAttribute({
        collection,
        id,
        attribute,
        updatedAt,
        value,
        min,
        max
    }: IncreaseDocumentAttribute): Promise<boolean> {
        const name = this.$.quote(collection);
        const attr = this.$.quote(attribute);
        const params: any[] = [value, updatedAt, id];

        let sql = `
            UPDATE ${this.getSQLTable(name)} 
            SET 
                ${attr} = ${attr} + ?,
                ${this.$.quote('_updatedAt')} = ?
            WHERE _uid = ?
            ${this.getTenantQuery(collection)}
        `;

        if (this.$sharedTables) {
            params.push(this.$tenantId);
        }
        if (max !== undefined && max !== null) {
            sql += ` AND ${attr} <= ?`;
            params.push(max);
        }
        if (min !== undefined && max !== null) {
            sql += ` AND ${attr} >= ?`;
            params.push(min);
        }

        sql = this.trigger(EventsEnum.DocumentUpdate, sql);

        try {
            await this.client.query(sql, params);
            return true;
        } catch (e: any) {
            throw this.processException(e, 'Failed to increase document attribute');
        }
    }

    public async find<D extends Doc>({
        collection,
        query = [],
        options = {}
    }: Find): Promise<D[]> {
        const {
            limit = 25,
            offset,
            orderAttributes = [],
            orderTypes = [],
            cursor = {},
            cursorDirection = CursorEnum.After,
            permission = PermissionEnum.Read
        } = options;

        const name = this.sanitize(collection);
        const roles = Authorization.getRoles();
        const where: string[] = [];
        const orders: string[] = [];
        const params: (string | number | undefined | null)[] = [];
        const alias = Query.DEFAULT_ALIAS;

        const queries = [...(Array.isArray(query) ? query : query(new QueryBuilder()).build())];

        const cursorWhere: string[] = [];

        orderAttributes.forEach((originalAttribute, i) => {
            let attribute = this.getInternalKeyForAttribute(originalAttribute);
            attribute = this.sanitize(attribute);

            let orderType = orderTypes[i] ?? OrderEnum.Asc;
            let direction = orderType;

            if (cursorDirection === CursorEnum.Before) {
                direction = (direction === OrderEnum.Asc)
                    ? OrderEnum.Desc
                    : OrderEnum.Asc;
            }

            orders.push(`${this.$.quote(attribute)} ${direction}`);

            // Build pagination WHERE clause only if we have a cursor
            if (cursor && Object.keys(cursor).length) {
                // Special case: No tie breaks. only 1 attribute and it's a unique primary key
                if (orderAttributes.length === 1 && i === 0 && originalAttribute === '$sequence') {
                    const operator = (direction === OrderEnum.Desc)
                        ? QueryType.LessThan
                        : QueryType.GreaterThan;

                    cursorWhere.push(`${this.$.quote(alias)}.${this.$.quote(attribute)} ${this.getSQLOperator(operator)} ?`);
                    params.push(cursor[originalAttribute]);
                    return;
                }

                const conditions: string[] = [];

                // Add equality conditions for previous attributes
                for (let j = 0; j < i; j++) {
                    const prevOriginal = orderAttributes[j]!;
                    const prevAttr = this.sanitize(this.getInternalKeyForAttribute(prevOriginal));

                    conditions.push(`${this.$.quote(alias)}.${this.$.quote(prevAttr)} = ?`);
                    params.push(cursor[prevOriginal]);
                }

                // Add comparison for current attribute
                const operator = (direction === OrderEnum.Desc)
                    ? QueryType.LessThan
                    : QueryType.GreaterThan;

                conditions.push(`${this.$.quote(alias)}.${this.$.quote(attribute)} ${this.getSQLOperator(operator)} ?`);
                params.push(cursor[originalAttribute]);

                cursorWhere.push(`(${conditions.join(' AND ')})`);
            }
        });

        if (cursorWhere.length) {
            where.push('(' + cursorWhere.join(' OR ') + ')');
        }

        const conditions = this.getSQLConditions(queries, params);
        if (conditions) {
            where.push(conditions);
        }

        if (Authorization.getStatus()) {
            where.push(this.getSQLPermissionsCondition({
                collection: name, roles, alias, type: permission
            }));
            if (this.$sharedTables) params.push(this.$tenantId);
        }

        if (this.$sharedTables) {
            params.push(this.$tenantId);
            where.push(this.getTenantQuery(collection, alias, undefined, ''));
        }

        const sqlWhere = where.length ? 'WHERE ' + where.join(' AND ') : '';
        const sqlOrder = 'ORDER BY ' + orders.join(', ');

        let sqlLimit = '';
        if (limit !== null && limit !== undefined) {
            params.push(limit);
            sqlLimit = 'LIMIT ?';
        }

        if (offset !== null && offset !== undefined) {
            params.push(offset);
            sqlLimit += ' OFFSET ?';
        }

        const selections = this.getAttributeSelections(queries);

        let sql = `
            SELECT ${this.getAttributeProjection(selections, alias)}
            FROM ${this.getSQLTable(name)} AS ${this.$.quote(alias)}
            ${sqlWhere}
            ${sqlOrder}
            ${sqlLimit}
        `;
        sql = this.trigger(EventsEnum.DocumentFind, sql);

        try {
            const { rows } = await this.client.query(sql, params);

            const results = rows.map((document: any, index: number) => {
                if ('_uid' in document) {
                    document['$id'] = document['_uid'];
                    delete document['_uid'];
                }
                if ('_id' in document) {
                    document['$sequence'] = document['_id'];
                    delete document['_id'];
                }
                if ('_tenant' in document) {
                    document['$tenant'] = document['_tenant'];
                    delete document['_tenant'];
                }
                if ('_createdAt' in document) {
                    document['$createdAt'] = document['_createdAt'];
                    delete document['_createdAt'];
                }
                if ('_updatedAt' in document) {
                    document['$updatedAt'] = document['_updatedAt'];
                    delete document['_updatedAt'];
                }
                if ('_permissions' in document) {
                    const _permissions = document['_permissions'];
                    try {
                        if (typeof _permissions === 'string') {
                            document['$permissions'] = JSON.parse(document['_permissions'] ?? '[]');
                        } else if (Array.isArray(_permissions)) {
                            document['$permissions'] = _permissions;
                        } else {
                            this.$logger.warn(`Unexpected type for _permissions: ${typeof _permissions}. Expected string or array.`);
                            document['$permissions'] = [];
                        }
                    } catch {
                        document['$permissions'] = [];
                    }
                    delete document['_permissions'];
                }

                return Doc.from(document);
            });

            if (cursorDirection === CursorEnum.Before) {
                results.reverse();
            }

            return results as any;
        } catch (e: any) {
            throw this.processException(e, 'Failed to find documents');
        }
    }

    public async count(
        collection: string,
        queries: ((b: QueryBuilder) => QueryBuilder) | Array<Query> = [],
        max?: number
    ): Promise<number> {
        const name = this.sanitize(collection);
        const roles = Authorization.getRoles();
        const params: any[] = [];
        const where: string[] = [];
        const alias = Query.DEFAULT_ALIAS;

        const queryList = [...(Array.isArray(queries) ? queries : queries(new QueryBuilder()).build())];

        const conditions = this.getSQLConditions(queryList, params);
        if (conditions) {
            where.push(conditions);
        }

        if (Authorization.getStatus()) {
            where.push(this.getSQLPermissionsCondition({
                collection: name, roles, alias, type: PermissionEnum.Read
            }));
            if (this.$sharedTables) params.push(this.$tenantId);
        }

        if (this.$sharedTables) {
            params.push(this.$tenantId);
            where.push(this.getTenantQuery(collection, alias, undefined, ''));
        }

        let limit = '';
        if (max !== null && max !== undefined) {
            params.push(max);
            limit = 'LIMIT ?';
        }

        const sqlWhere = where.length > 0
            ? 'WHERE ' + where.join(' AND ')
            : '';

        let sql = `
            SELECT COUNT(1) as sum FROM (
                SELECT 1
                FROM ${this.getSQLTable(name)} AS ${this.$.quote(alias)}
                ${sqlWhere}
                ${limit}
            ) table_count
        `;

        sql = this.trigger(EventsEnum.DocumentCount, sql);

        try {
            const { rows } = await this.client.query<any>(sql, params);
            const result = rows[0];
            return result?.sum ?? 0;
        } catch (error) {
            throw this.processException(error, 'Failed to count documents');
        }
    }

    public async sum(
        collection: string,
        attribute: string,
        queries: ((b: QueryBuilder) => QueryBuilder) | Array<Query> = [],
        max?: number
    ): Promise<number> {
        const name = this.sanitize(collection);
        const roles = Authorization.getRoles();
        const params: any[] = [];
        const where: string[] = [];
        const alias = Query.DEFAULT_ALIAS;

        const queryList = [...(Array.isArray(queries) ? queries : queries(new QueryBuilder()).build())];

        const conditions = this.getSQLConditions(queryList, params);
        if (conditions) {
            where.push(conditions);
        }

        if (Authorization.getStatus()) {
            where.push(this.getSQLPermissionsCondition({
                collection: name, roles, alias, type: PermissionEnum.Read
            }));
            if (this.$sharedTables) params.push(this.$tenantId);
        }

        if (this.$sharedTables) {
            params.push(this.$tenantId);
            where.push(this.getTenantQuery(collection, alias, undefined, ''));
        }

        let limit = '';
        if (max !== null && max !== undefined) {
            params.push(max);
            limit = 'LIMIT ?';
        }

        const sqlWhere = where.length > 0
            ? 'WHERE ' + where.join(' AND ')
            : '';

        let sql = `
            SELECT SUM(${this.$.quote(attribute)}) as sum FROM (
                SELECT ${this.$.quote(attribute)}
                FROM ${this.getSQLTable(name)} AS ${this.$.quote(alias)}
                ${sqlWhere}
                ${limit}
            ) table_count
        `;

        sql = this.trigger(EventsEnum.DocumentSum, sql);

        try {
            const { rows } = await this.client.query<any>(sql, params);
            const result = rows[0];
            return result?.sum ?? 0;
        } catch (error) {
            throw this.processException(error, 'Failed to sum documents');
        }
    }

    protected async updatePermissions(
        collection: string,
        document: Doc,
    ) {
        let removePermissions: { sql: string, params: any[] } = { sql: '', params: [] };
        let addPermissions: { sql: string, params: any[] } = { sql: '', params: [] };

        const sqlParams: any[] = [document.getId()];
        let sql = `
			SELECT _type, _permission
			FROM ${this.getSQLTable(collection + '_perms')}
			WHERE _document = ?
            ${this.getTenantQuery(collection)}
        `;
        sql = this.trigger(EventsEnum.PermissionsRead, sql);

        if (this.$sharedTables) {
            sqlParams.push(this.$tenantId);
        }

        const { rows } = await this.client.query<any>(sql, sqlParams);

        const initial: Record<string, string[]> = {};
        for (const type of Database.PERMISSIONS) {
            initial[type] = [];
        }

        const permissions = rows.reduce((carry: Record<string, string[]>, item: any) => {
            const type = item._type;
            if (!carry[type]) {
                carry[type] = [];
            }
            carry[type].push(item._permission);
            return carry;
        }, initial);

        const removals: Record<string, string[]> = {};
        for (const type of Database.PERMISSIONS) {
            const diff = permissions[type]!.filter((perm: string) => !document.getPermissionsByType(type).includes(perm));
            if (diff.length > 0) {
                removals[type] = diff;
            }
        }

        const additions: Record<string, string[]> = {};
        for (const type of Database.PERMISSIONS) {
            const diff = document.getPermissionsByType(type).filter((perm: string) => !permissions[type]!.includes(perm));
            if (diff.length > 0) {
                additions[type] = diff;
            }
        }

        if (Object.keys(removals).length > 0) {
            let removeQuery = ' AND (';
            const removeParams: any[] = [document.getId()];
            if (this.$sharedTables) {
                removeParams.push(this.$tenantId);
            }

            const typeConditions: string[] = [];
            for (const [type, perms] of Object.entries(removals)) {
                const placeholders = perms.map(() => '?').join(', ');
                typeConditions.push(`(_type = ? AND _permission IN (${placeholders}))`);
                removeParams.push(type);
                removeParams.push(...perms);
            }
            removeQuery += typeConditions.join(' OR ');
            removeQuery += ')';

            let removeSQL = `
                        DELETE FROM ${this.getSQLTable(collection + '_perms')}
                        WHERE _document = ?
                        ${this.getTenantQuery(collection)}
                    `;
            removeSQL += removeQuery;
            removeSQL = this.trigger(EventsEnum.PermissionsDelete, removeSQL);

            removePermissions = { sql: removeSQL, params: removeParams };
        }

        // Query to add permissions
        if (Object.keys(additions).length > 0) {
            const values: string[] = [];
            const addParams: any[] = [];

            for (const [type, perms] of Object.entries(additions)) {
                for (const permission of perms) {
                    if (this.$sharedTables) {
                        values.push('(?, ?, ?, ?)');
                        addParams.push(document.getId(), type, permission, this.$tenantId);
                    } else {
                        values.push('(?, ?, ?)');
                        addParams.push(document.getId(), type, permission);
                    }
                }
            }

            let addSQL = `
                INSERT INTO ${this.getSQLTable(collection + '_perms')} (_document, _type, _permission
            `;

            if (this.$sharedTables) {
                addSQL += ', _tenant)';
            } else {
                addSQL += ')';
            }

            addSQL += ` VALUES ${values.join(', ')}`;
            addSQL = this.trigger(EventsEnum.PermissionsCreate, addSQL);

            addPermissions = { sql: addSQL, params: addParams };
        }

        return { addPermissions, removePermissions };
    }

    protected getSQLType(type: AttributeEnum, size?: number, array?: boolean): string {
        let pgType: string;
        size ??= 0;

        switch (type) {
            case AttributeEnum.String:
                if (size > 255) {
                    pgType = 'TEXT';
                } else {
                    pgType = `VARCHAR(${size})`;
                }
                break;
            case AttributeEnum.Integer:
                if (size <= 2) { // Roughly fits SMALLINT (-32768 to +32767)
                    pgType = 'SMALLINT';
                } else if (size <= 4) { // Roughly fits INTEGER (-2147483648 to +2147483647)
                    pgType = 'INTEGER';
                } else { // For larger integers, BIGINT is appropriate
                    pgType = 'BIGINT';
                }
                break;
            case AttributeEnum.Float:
                pgType = 'DOUBLE PRECISION';
                break;
            case AttributeEnum.Boolean:
                pgType = 'BOOLEAN';
                break;
            case AttributeEnum.Timestamptz:
                pgType = 'TIMESTAMP WITH TIME ZONE';
                break;
            case AttributeEnum.Relationship:
                pgType = 'VARCHAR(255)';
                break;
            case AttributeEnum.Json:
                pgType = 'JSONB';
                break;
            case AttributeEnum.Virtual:
                pgType = '';
                break;
            case AttributeEnum.Uuid:
                pgType = 'UUID';
                break;
            default:
                throw new DatabaseException(`Unsupported attribute type: ${type}`);
        }

        if (array && pgType) {
            return `${pgType}[]`;
        } else {
            return pgType;
        }
    }

    protected getIndexName(coll: string, id: string): string {
        return `${this.sanitize(coll)}_${this.sanitize(id)}`;
    }

    protected getSQLCondition(query: Query, binds: any[]): string {
        query.setAttribute(this.getInternalKeyForAttribute(query.getAttribute()));

        const attribute = this.quote(this.sanitize(query.getAttribute()));
        const alias = this.quote(Query.DEFAULT_ALIAS);
        const method = query.getMethod();

        switch (method) {
            case QueryType.Or:
            case QueryType.And:
                const conditions: string[] = [];
                for (const q of query.getValues() as Query[]) {
                    conditions.push(this.getSQLCondition(q, binds));
                }

                const methodStr = method.toUpperCase();
                return conditions.length === 0 ? '' : ` ${methodStr} (` + conditions.join(' AND ') + ')';

            case QueryType.Search:
                binds.push(this.getFulltextValue(query.getValue() as string));
                return `MATCH(${alias}.${attribute}) AGAINST (? IN BOOLEAN MODE)`;

            case QueryType.Between:
                const values = query.getValues();
                binds.push(values[0], values[1]);
                return `${alias}.${attribute} BETWEEN ? AND ?`;

            case QueryType.IsNull:
            case QueryType.IsNotNull:
                return `${alias}.${attribute} ${this.getSQLOperator(method)}`;

            case QueryType.Contains:
                if (this.$supportForJSONOverlaps && query.onArray()) {
                    binds.push(JSON.stringify(query.getValues()));
                    return `JSON_OVERLAPS(${alias}.${attribute}, ?)`;
                }
            // Fall through to default case

            default:
                const defaultConditions: string[] = [];
                for (const value of query.getValues() as string[]) {
                    let processedValue = value;
                    switch (method) {
                        case QueryType.StartsWith:
                            processedValue = this.escapeWildcards(value) + '%';
                            break;
                        case QueryType.EndsWith:
                            processedValue = '%' + this.escapeWildcards(value);
                            break;
                        case QueryType.Contains:
                            processedValue = query.onArray()
                                ? JSON.stringify(value)
                                : '%' + this.escapeWildcards(value) + '%';
                            break;
                    }

                    binds.push(processedValue);
                    defaultConditions.push(`${alias}.${attribute} ${this.getSQLOperator(method)} ?`);
                }

                return defaultConditions.length === 0 ? '' : '(' + defaultConditions.join(' OR ') + ')';
        }
    }

    protected processException(error: any, message?: string): never {
        console.log({ error, message })
        throw new DatabaseException('Not implemented')
    }

    readonly $supportForTimeouts = true;
    public get $internalIndexesKeys() {
        return ['primary', '_created_at', '_updated_at', '_tenant_id'];
    }

    public setTimeout(milliseconds: number, event: EventsEnum = EventsEnum.All): void {
        if (!this.$supportForTimeouts) {
            return;
        }
        if (milliseconds <= 0) {
            throw new DatabaseException('Timeout must be greater than 0');
        }

        this.$timeout = milliseconds;

        const seconds = milliseconds / 1000;

        this.before(event, 'timeout', (sql: string) => {
            return `SET STATEMENT max_statement_time = ${seconds} FOR ${sql}`;
        });
    }

    protected getSQLOperator(method: string): string {
        switch (method) {
            case QueryType.Equal:
                return '=';
            case QueryType.NotEqual:
                return '!=';
            case QueryType.LessThan:
                return '<';
            case QueryType.LessThanEqual:
                return '<=';
            case QueryType.GreaterThan:
                return '>';
            case QueryType.GreaterThanEqual:
                return '>=';
            case QueryType.IsNull:
                return 'IS NULL';
            case QueryType.IsNotNull:
                return 'IS NOT NULL';
            case QueryType.StartsWith:
            case QueryType.EndsWith:
            case QueryType.Contains:
                return 'LIKE';
            default:
                throw new DatabaseException('Unknown method: ' + method);
        }
    }

    protected getSQLTable(name: string): string {
        if (!name) {
            throw new DatabaseException("Failed to get SQL table: name is empty");
        }
        return `${this.$.quote(this.$schema)}.${this.$.quote(`${this.$namespace}_${name}`)}`;
    }

    protected getSQLIndexType(type: IndexEnum): string {
        switch (type) {
            case IndexEnum.Unique:
                return 'UNIQUE';
            case IndexEnum.FullText:
                return 'FULLTEXT';
            case IndexEnum.Key:
                return 'INDEX';
            default:
                throw new DatabaseException(`Unsupported index type: ${type}`);
        }
    }

    protected getSQLPermissionsCondition({
        collection,
        roles,
        alias,
        type = PermissionEnum.Read
    }: {
        collection: string;
        roles: string[];
        alias: string;
        type?: PermissionEnum;
    }): string {
        if (!collection || !roles?.length || !alias) {
            throw new DatabaseException("Failed to get SQL permission condition: collection, roles, and alias are required");
        }

        if (type && !Object.values(PermissionEnum).includes(type)) {
            throw new DatabaseException(`Unknown permission type: ${type}`);
        }

        const quotedRolesArray = `ARRAY[${roles.map(role => this.client.quote(role)).join(', ')}]::text[]`;

        return `
        ${this.quote(alias)}.${this.quote('_id')} IN (
            SELECT ${this.quote('_document')}
            FROM ${this.getSQLTable(`${collection}_perms`)}
            WHERE ${this.quote('_permissions')} && ${quotedRolesArray}
              AND ${this.quote('_type')} = ${this.client.quote(type)}
              ${this.getTenantQuery(collection)}
            )
        `.trim();
    }

    /**
     * Builds SQL conditions recursively and mutates the provided `binds` array with bound values.
     * @returns SQL condition string with placeholders.
    */
    protected getSQLConditions(queries: Query[], binds: any[], separator: string = 'AND'): string {
        const conditions: string[] = [];

        for (const query of queries) {
            if (query.getMethod() === QueryType.Select) {
                continue;
            }

            if (query.isNested()) {
                conditions.push(this.getSQLConditions(query.getValues() as Query[], binds, query.getMethod()));
            } else {
                conditions.push(this.getSQLCondition(query, binds));
            }
        }

        const tmp = conditions.join(` ${separator} `);
        return tmp === '' ? '' : `(${tmp})`;
    }

    protected getTenantQuery(
        collection: string,
        alias: string = '',
        tenantCount: number = 0,
        condition: string = 'AND'
    ): string {
        if (!this.$sharedTables) {
            return '';
        }

        let dot = '';
        let quotedAlias = alias;

        if (alias !== '') {
            dot = '.';
            quotedAlias = this.quote(alias);
        }

        let bindings: string[] = [];
        if (tenantCount === 0) {
            bindings.push('?');
        } else {
            bindings = Array.from({ length: tenantCount }, _ => `?`);
        }
        const bindingsStr = bindings.join(',');

        let orIsNull = '';
        if (collection === Database.METADATA) {
            orIsNull = ` OR ${quotedAlias}${dot}${this.quote('_tenant')} IS NULL`;
        }

        return `${condition} (${quotedAlias}${dot}${this.quote('_tenant')} IN (${bindingsStr})${orIsNull})`;
    }

    protected getAttributeProjection(selections: string[], prefix: string): string {
        if (!selections.length) throw new DatabaseException('Selections are required internally.');

        const projected: string[] = [];
        selections.splice(0, 0,
            "$id",
            "$sequence",
            "$createdAt",
            "$updatedAt",
            "$permissions",
        )

        for (let key of selections) {
            if (key === '$collection') continue;

            let dbKey = this.getInternalKeyForAttribute(key);
            let alias = key;

            projected.push(`${this.quote(prefix)}.${this.quote(dbKey)} AS ${this.quote(alias)}`);
        }

        return projected.join(', ');
    }

    public quote(name: string): string {
        if (!name) {
            throw new DatabaseException("Failed to quote name: name is empty");
        }
        return `"${name}"`;
    }

    protected getAttributeSelections(queries: QueryBuilder | Array<Query>): string[] {
        const selections: string[] = [];
        queries = Array.isArray(queries) ? queries : queries.build();

        for (const query of queries) {
            if (query.getMethod() === QueryType.Select) {
                selections.push(...query.getValues() as string[]);
            }
        }

        return selections;
    }

    protected getInternalKeyForAttribute(attribute: string): string {
        switch (attribute) {
            case '$id':
                return '_uid';
            case '$sequence':
                return '_id';
            case '$collection':
                return '_collection';
            case '$tenant':
                return '_tenant';
            case '$createdAt':
                return '_createdAt';
            case '$updatedAt':
                return '_updatedAt';
            case '$permissions':
                return '_permissions';
            default:
                return attribute;
        }
    }

    protected getFulltextValue(value: string): string {
        const exact = value.startsWith('"') && value.endsWith('"');

        // Replace reserved chars with space
        const specialChars = ['@', '+', '-', '*', ')', '(', ',', '<', '>', '~', '"'];
        let sanitized = value;
        for (const char of specialChars) {
            sanitized = sanitized.split(char).join(' ');
        }
        sanitized = sanitized.replace(/\s+/g, ' ').trim();

        if (!sanitized) {
            return '';
        }

        if (exact) {
            sanitized = `"${sanitized}"`;
        } else {
            sanitized += '*';
        }

        return sanitized;
    }

    protected escapeWildcards(value: string): string {
        const wildcards = ['%', '_', '[', ']', '^', '-', '.', '*', '+', '?', '(', ')', '{', '}', '|'];

        for (const wildcard of wildcards) {
            value = value.replace(new RegExp('\\' + wildcard.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'g'), '\\' + wildcard);
        }

        return value;
    }

    protected static POSTGRES_ROW_OVERHEAD_MIN = 24;
    protected static POSTGRES_TOAST_POINTER_SIZE = 20;

    public getAttributeWidth(collection: Doc<Collection>): number {
        let totalEstimatedBytes = BaseAdapter.POSTGRES_ROW_OVERHEAD_MIN;

        // Base columns in the main collection table:
        // "_id" BIGINT: 8 bytes
        // "_uid" VARCHAR(255): 255 (actual data) + 1 (length byte for short strings) = 256 bytes *or* 4 (length byte) + 255 if long string.
        //     For estimating, we often assume max storage. For varchar(255), in-row is often 256.
        // "_createdAt" TIMESTAMP WITH TIME ZONE: 8 bytes
        // "_updatedAt" TIMESTAMP WITH TIME ZONE: 8 bytes
        // "_permissions" TEXT[]: This is an array, so it will be TOASTed if it gets large. 20-byte pointer.

        // Shared table `_tenant` (INTEGER): 4 bytes

        // _id (BIGINT)
        totalEstimatedBytes += 8;
        // _uid (VARCHAR(255)) - for in-row storage, it's roughly actual_length + 1 byte for small, 4 bytes for large.
        // For max length varchar, it will likely be 255 + 1. Let's assume max length for estimation.
        totalEstimatedBytes += 256; // 255 (data) + 1 (length header for small varlena)
        // _createdAt (TIMESTAMP WITH TIME ZONE)
        totalEstimatedBytes += 8;
        // _updatedAt (TIMESTAMP WITH TIME ZONE)
        totalEstimatedBytes += 8;
        totalEstimatedBytes += BaseAdapter.POSTGRES_TOAST_POINTER_SIZE;
        totalEstimatedBytes += 4;

        // Count of fixed columns for NULL bitmap
        let numberOfColumns = 6; // _id, _uid, _createdAt, _updatedAt, _permissions, _tenant

        const attributes = collection.get('attributes', []);

        for (const attr of attributes) {
            const attribute = attr.toObject();
            numberOfColumns++;

            if (attribute.array ?? false) {
                totalEstimatedBytes += BaseAdapter.POSTGRES_TOAST_POINTER_SIZE;
                continue;
            }

            switch (attribute.type) {
                case AttributeEnum.String:
                    attribute.size = attribute?.size ?? 255;

                    if (attribute.size > this.$maxVarcharLength || attribute.size > 255) {
                        totalEstimatedBytes += BaseAdapter.POSTGRES_TOAST_POINTER_SIZE;
                    } else {
                        // VARCHAR(<=255). It will be in-row.
                        // Actual data size + 1 byte for header (if < 128 bytes) or 4 bytes for header (if >= 128 bytes).
                        totalEstimatedBytes += attribute.size + 1;
                    }
                    break;

                case AttributeEnum.Integer:
                    attribute.size = attribute?.size ?? 4;
                    if (attribute.size <= 2) {
                        totalEstimatedBytes += 2; // SMALLINT
                    } else if (attribute.size <= 4) {
                        totalEstimatedBytes += 4; // INTEGER
                    } else { // >= 8
                        totalEstimatedBytes += 8; // BIGINT
                    }
                    break;

                case AttributeEnum.Float:
                    totalEstimatedBytes += 8;
                    break;

                case AttributeEnum.Boolean:
                    totalEstimatedBytes += 1;
                    break;

                case AttributeEnum.Relationship:
                    totalEstimatedBytes += 256;
                    break;

                case AttributeEnum.Timestamptz:
                    // TIMESTAMP WITH TIME ZONE (8 bytes)
                    totalEstimatedBytes += 8;
                    break;

                case AttributeEnum.Json:
                    totalEstimatedBytes += BaseAdapter.POSTGRES_TOAST_POINTER_SIZE;
                    break;

                case AttributeEnum.Uuid:
                    // UUID (16 bytes)
                    totalEstimatedBytes += 16;
                    break;

                case AttributeEnum.Virtual:
                    numberOfColumns--;
                    break;

                default:
                    throw new DatabaseException('Unknown attribute type: ' + attribute.type);
            }
        }

        // Add NULL bitmap size: (number_of_columns + 7) / 8, rounded up
        totalEstimatedBytes += Math.ceil(numberOfColumns / 8);
        return totalEstimatedBytes;
    }

    public getCountOfAttributes(collection: Doc<Collection>): number {
        const attributes = collection.get('attributes', []);
        return attributes.length + this.$countOfDefaultAttributes;
    }

    public getCountOfIndexes(collection: Doc<Collection>): number {
        const indexes = collection.get('indexes', []);
        return indexes.length + this.$countOfDefaultIndexes;
    }

    public get $countOfDefaultAttributes(): number {
        return Database.INTERNAL_ATTRIBUTES.length;
    }

    public get $countOfDefaultIndexes(): number {
        return Database.INTERNAL_INDEXES.length;
    }

    protected readonly $internalAttrs = [
        "$id",
        "$sequence",
        "$collection",
        "$tenant",
        "$createdAt",
        "$updatedAt",
        "$permissions",
    ]

}

export interface Meta {
    database: string;
    schema: string;
    sharedTables: boolean;
    tenantId: number;
    tenantPerDocument: boolean;
    namespace: string;
    metadata: Record<string, string>
}
