import { DatabaseException } from "@errors/base.js";
import { EventEmitter } from "stream";
import { IAdapter, IClient } from "./interface.js";
import { AttributeEnum, CursorEnum, EventsEnum, IndexEnum, OrderEnum, PermissionEnum } from "@core/enums.js";
import { CreateAttribute, Find, IncreaseDocumentAttribute } from "./types.js";
import { Doc } from "@core/doc.js";
import { Database } from "@core/database.js";
import { QueryBuilder } from "@utils/query-builder.js";
import { Query, QueryType } from "@core/query.js";
import { Entities, IEntity } from "types.js";
import { Logger } from "@utils/logger.js";
import { Authorization } from "@utils/authorization.js";
import { Collection, Index } from "@validators/schema.js";

export abstract class BaseAdapter extends EventEmitter {
    public readonly type: string = 'base';
    protected _meta: Partial<Meta> = {};
    protected abstract client: IClient;
    protected $logger = new Logger();

    protected $timeout: number = 0;

    readonly $limitForString: number = 4294967295;
    readonly $limitForInt: number = 4294967295;
    readonly $limitForAttributes: number = 1017;
    readonly $limitForIndexes: number = 64;
    readonly $supportForSchemas: boolean = true;
    readonly $supportForIndex: boolean = true;
    readonly $supportForAttributes: boolean = true;
    readonly $supportForUniqueIndex: boolean = true;
    readonly $supportForFulltextIndex: boolean = true;
    readonly $supportForUpdateLock: boolean = true;
    readonly $supportForAttributeResizing: boolean = true;
    readonly $supportForBatchOperations: boolean = true;
    readonly $supportForGetConnectionId: boolean = true;
    readonly $supportForCacheSkipOnFailure: boolean = true;
    readonly $supportForHostname: boolean = true;
    readonly $documentSizeLimit: number = 65535;
    readonly $supportForCasting: boolean = false;
    readonly $supportForNumericCasting: boolean = false;
    readonly $supportForQueryContains: boolean = true;
    readonly $supportForIndexArray: boolean = true;
    readonly $supportForCastIndexArray: boolean = false;
    readonly $supportForRelationships: boolean = true;
    readonly $supportForReconnection: boolean = true;
    readonly $supportForBatchCreateAttributes: boolean = true;
    readonly $maxVarcharLength: number = 16381;
    readonly $maxIndexLength: number = this.$sharedTables ? 767 : 768;


    protected transformations: Partial<Record<EventsEnum, Array<[string, (query: string) => string]>>> = {
        [EventsEnum.All]: [],
    }

    constructor(options: { type?: string } = {}) {
        super();
        if (options.type) {
            this.type = options.type;
        }
    }

    public get $database(): string {
        if (!this._meta.database) throw new DatabaseException('Database name is not defined in adapter metadata.');
        return this._meta.database;
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
            // Check if collection exists
            sql = `
                  SELECT COUNT(*) as count 
                  FROM information_schema.tables 
                  WHERE table_schema = ? 
                  AND table_name = ?
                `;
            values.push(this.sanitize(collection));
        } else {
            // Check if schema exists
            sql = `
                  SELECT COUNT(*) as count 
                  FROM information_schema.schemata 
                  WHERE schema_name = ?
                `;
        }

        const [result] = await this.client.query<any>(sql, values);
        return result[0].count > 0;
    }

    public async createAttribute(
        { name, collection, size, signed, array, type }: CreateAttribute
    ): Promise<void> {
        if (!name || !collection || !type) {
            throw new DatabaseException("Failed to create attribute: name, collection, and type are required");
        }

        const sqlType = this.getSQLType(type, size, signed, array);
        const table = this.getSQLTable(collection);

        let sql = `
            ALTER TABLE ${table}
            ADD COLUMN ${this.$.quote(name)} ${sqlType}
        `;
        sql = this.trigger(EventsEnum.AttributeCreate, sql);

        await this.client.query(sql);
    }

    public async createAttributes(
        collection: string,
        attributes: Omit<CreateAttribute, 'collection'>[]
    ): Promise<void> {
        if (!Array.isArray(attributes) || attributes.length === 0) {
            throw new DatabaseException("Failed to create attributes: attributes must be a non-empty array");
        }
        const parts: string[] = [];

        for (const attr of attributes) {
            if (!attr.name || !attr.type) {
                throw new DatabaseException("Failed to create attribute: name and type are required");
            }
            const sqlType = this.getSQLType(attr.type, attr.size, attr.signed, attr.array);
            parts.push(`${this.$.quote(attr.name)} ${sqlType}`);
        }

        const columns = parts.join(', ADD COLUMN ');
        const table = this.getSQLTable(collection);
        let sql = `
            ALTER TABLE ${table}
            ADD COLUMN ${columns}
        `;
        sql = this.trigger(EventsEnum.AttributesCreate, sql);

        await this.client.query(sql);
    }

    public async renameAttribute(
        collection: string,
        oldName: string,
        newName: string
    ): Promise<void> {
        if (!oldName || !newName || !collection) {
            throw new DatabaseException("Failed to rename attribute: oldName, newName, and collection are required");
        }

        const table = this.getSQLTable(collection);
        let sql = `
            ALTER TABLE ${table}
            RENAME COLUMN ${this.$.quote(oldName)} TO ${this.$.quote(newName)}
        `;
        sql = this.trigger(EventsEnum.AttributeUpdate, sql);

        await this.client.query(sql);
    }

    public async deleteAttribute(
        collection: string,
        name: string
    ): Promise<void> {
        if (!name || !collection) {
            throw new DatabaseException("Failed to delete attribute: name and collection are required");
        }

        const table = this.getSQLTable(collection);
        let sql = `
            ALTER TABLE ${table}
            DROP COLUMN ${this.$.quote(name)}
        `;
        sql = this.trigger(EventsEnum.AttributeDelete, sql);

        await this.client.query(sql);
    }

    public async getDocument<C extends keyof Entities>(
        collection: C,
        id: string,
        queries?: ((b: QueryBuilder) => QueryBuilder) | Array<Query>,
        forUpdate?: boolean
    ): Promise<Doc<Entities[C]>>;
    public async getDocument(
        collection: string,
        id: string,
        queries: ((b: QueryBuilder) => QueryBuilder) | Array<Query> = [],
        forUpdate: boolean = false
    ): Promise<Doc<IEntity>> {
        if (!collection || !id) {
            throw new DatabaseException("Failed to get document: collection and id are required");
        }
        queries = Array.isArray(queries) ? queries : queries(new QueryBuilder()).build();

        const table = this.getSQLTable(collection);
        const selections = this.getAttributeSelections(queries);
        const alias = Query.DEFAULT_ALIAS;
        const params: any[] = [id];

        let sql = `
            SELECT ${this.getAttributeProjection(selections, alias)}
            FROM ${table} AS ${alias}
            WHERE ${this.$.quote(alias)}.${this.$.quote('_uid')} = ?
            ${this.getTenantQuery(collection, alias)}
        `;

        if (forUpdate && this.$supportForUpdateLock) {
            sql += ' FOR UPDATE';
        }

        if (this.$sharedTables) {
            params.push(this.$tenantId);
        }

        const [rows] = await this.client.query<any>(sql, params);

        let document = rows[0];

        if ('_id' in document) {
            document['$sequence'] = document['_id'];
            delete document['_id'];
        }
        if ('_uid' in document) {
            document['$id'] = document['_uid'];
            delete document['_uid'];
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

            const [result] = await this.client.query<any>(sql, params);

            // Delete permissions
            const permParams: any[] = [id];
            let permSql = `
                DELETE FROM ${this.getSQLTable(collection + '_perms')}
                WHERE ${this.$.quote('_document')} = ?
                ${this.getTenantQuery(collection)}
            `;

            permSql = this.trigger(EventsEnum.PermissionsDelete, permSql);

            if (this.$sharedTables) {
                permParams.push(this.$tenantId);
            }

            await this.client.query(permSql, permParams);

            return result.affectedRows > 0; // TODO: #type
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
            const [rows]: any = await this.client.query(sql, params);

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

            return results;
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
            const [rows] = await this.client.query<any>(sql, params);
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
            const [rows] = await this.client.query<any>(sql, params);
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

        const [rows] = await this.client.query<any>(sql, sqlParams);

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
            const diff = permissions[type].filter((perm: string) => !document.getPermissionsByType(type).includes(perm));
            if (diff.length > 0) {
                removals[type] = diff;
            }
        }

        const additions: Record<string, string[]> = {};
        for (const type of Database.PERMISSIONS) {
            const diff = document.getPermissionsByType(type).filter((perm: string) => !permissions[type].includes(perm));
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

    protected abstract getSQLType(type: AttributeEnum, size: number, signed?: boolean, array?: boolean): string;
    protected abstract processException(error: any, message?: string): never;
    protected abstract getLikeOperator(): string;
    protected abstract getSQLCondition(query: Query, binds: any[]): string;

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
                return this.getLikeOperator();
            default:
                throw new DatabaseException('Unknown method: ' + method);
        }
    }

    protected getSQLTable(name: string): string {
        if (!name) {
            throw new DatabaseException("Failed to get SQL table: name is empty");
        }
        return `${this.$.quote(this.$database)}.${this.$.quote(`${this.$namespace}_${name}`)}`;
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

    protected getSQLPermissionsCondition(
        { collection, roles, alias, type = PermissionEnum.Read }: { collection: string, roles: string[], alias: string, type?: PermissionEnum }
    ): string {
        if (!collection || !roles || !alias) {
            throw new DatabaseException("Failed to get SQL permission condition: collection, roles, and alias are required");
        }

        if (type && !Object.values(PermissionEnum).includes(type)) {
            throw new DatabaseException(`Unknown permission type: ${type}`);
        }

        const quotedRoles = roles.map(role => this.client.quote(role)).join(', ');

        return `${this.$.quote(alias)}.${this.$.quote('_uid')} IN (
            SELECT _document
            FROM ${this.getSQLTable(collection + '_perms')}
            WHERE _permission IN (${quotedRoles})
              AND _type = '${type}'
              ${this.getTenantQuery(collection)}
        )`;
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

    public getTenantQuery(
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
            quotedAlias = this.$.quote(alias);
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
            orIsNull = ` OR ${quotedAlias}${dot}_tenant IS NULL`;
        }

        return `${condition} (${quotedAlias}${dot}_tenant IN (${bindingsStr})${orIsNull})`;
    }

    protected getAttributeProjection(selections: string[], prefix: string): string {
        if (!selections || selections.length === 0 || selections.includes('*')) {
            return `${this.$.quote(prefix)}.*`;
        }

        const internalKeys = [
            '$id',
            '$sequence',
            '$permissions',
            '$createdAt',
            '$updatedAt',
        ];

        // Remove internal keys and $collection from selections
        selections = selections.filter(
            (s) => ![...internalKeys, '$collection'].includes(s)
        );

        // Add internal keys as their mapped SQL names
        for (const internalKey of internalKeys) {
            selections.push(this.getInternalKeyForAttribute(internalKey));
        }

        const projected = selections.map(
            (selection) =>
                `${this.$.quote(prefix)}.${this.$.quote(selection)}`
        );

        return projected.join(',');
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

    public getAttributeWidth(collection: Doc<Collection>): number {
        /**
         * @link https://dev.mysql.com/doc/refman/8.0/en/storage-requirements.html
         *
         * `_id` bigint => 8 bytes
         * `_uid` varchar(255) => 1021 (4 * 255 + 1) bytes
         * `_tenant` int => 4 bytes
         * `_createdAt` datetime(3) => 7 bytes
         * `_updatedAt` datetime(3) => 7 bytes
         * `_permissions` mediumtext => 20
         */

        let total = 1067;

        const attributes = collection.get('attributes', []);

        for (const attr of attributes) {
            const attribute = attr.toObject();
            /**
             * Json / Longtext
             * only the pointer contributes 20 bytes
             * data is stored externally
             */

            if (attribute.array ?? false) {
                total += 20;
                continue;
            }

            switch (attribute.type) {
                case AttributeEnum.String:
                    /**
                     * Text / Mediumtext / Longtext
                     * only the pointer contributes 20 bytes to the row size
                     * data is stored externally
                     */
                    attribute.size = attribute?.size ?? 255;
                    if (attribute?.size > this.$maxVarcharLength) {
                        total += 20;
                    } else if (attribute.size > 255) {
                        total += attribute.size * 4 + 2; // VARCHAR(>255) + 2 length
                    } else {
                        total += attribute.size * 4 + 1; // VARCHAR(<=255) + 1 length
                    }
                    break;

                case AttributeEnum.Integer:
                    attribute.size = attribute?.size ?? 4;
                    if (attribute.size >= 8) {
                        total += 8; // BIGINT 8 bytes
                    } else {
                        total += 4; // INT 4 bytes
                    }
                    break;

                case AttributeEnum.Float:
                    total += 8; // DOUBLE 8 bytes
                    break;

                case AttributeEnum.Boolean:
                    total += 1; // TINYINT(1) 1 bytes
                    break;

                case AttributeEnum.Relationship:
                    total += Database.LENGTH_KEY * 4 + 1; // VARCHAR(<=255)
                    break;

                case AttributeEnum.Datetime:
                    /**
                     * 1 byte year + month
                     * 1 byte for the day
                     * 3 bytes for the hour, minute, and second
                     * 2 bytes miliseconds DATETIME(3)
                     */
                    total += 7;
                    break;
                case AttributeEnum.Object:
                    total += 20; // JSON / LONGTEXT pointer
                    break;
                case AttributeEnum.Virtual:
                    break; // Virtual attributes do not contribute to the row size
                default:
                    throw new DatabaseException('Unknown type: ' + attribute.type);
            }
        }

        return total;
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

export type Adapter = BaseAdapter & IAdapter;
