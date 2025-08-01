import { DatabaseException } from "@errors/base.js";
import { EventEmitter } from "stream";
import { IAdapter, IClient } from "./interface.js";
import { AttributeEnum, EventsEnum, IndexEnum, PermissionEnum } from "@core/enums.js";
import { CreateAttribute } from "./types.js";
import { Doc } from "@core/doc.js";
import { Database } from "@core/database.js";
import { QueryBuilder } from "@utils/query-builder.js";
import { Query, QueryType } from "@core/query.js";
import { IEntity } from "types.js";
import { Logger } from "@utils/logger.js";

export abstract class BaseAdapter extends EventEmitter {
    public readonly type: string = 'base';
    protected _meta: Partial<Meta> = {};
    protected abstract client: IClient;
    protected $logger = new Logger();

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
            throw new DatabaseException('Shared tables are enabled but tenantId is not defined in adapter metadata.');
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

    protected sanitize(value: string): string {
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


    protected abstract getSQLType(type: AttributeEnum, size: number, signed?: boolean, array?: boolean): string;

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

    protected getSQLPermissionCondition(
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
              ${this.getTenantQuery ? this.getTenantQuery(collection) : ''}
        )`;
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
                selections.push(...query.getValues());
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

    public getCountOfAttributes(collection: Doc): number {
        const attributes = collection.get('attributes', []);
        return attributes.length + this.$countOfDefaultAttributes;
    }

    public getCountOfIndexes(collection: Doc): number {
        const indexes = collection.get('indexes', []);
        return indexes.length + this.$countOfDefaultIndexes;
    }

    public get $countOfDefaultAttributes(): number {
        return Database.INTERNAL_ATTRIBUTES.length;
    }

    public get $countOfDefaultIndexes(): number {
        return Database.INTERNAL_INDEXES.length;
    }
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
