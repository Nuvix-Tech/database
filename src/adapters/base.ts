import { DatabaseException } from "@errors/base.js";
import { EventEmitter } from "stream";
import { IAdapter, IClient } from "./interface.js";
import { AttributeEnum, EventsEnum } from "@core/enums.js";
import { CreateAttribute } from "./types.js";

export abstract class BaseAdapter extends EventEmitter {
    public readonly type: string = 'base';
    protected _meta: Partial<Meta> = {};
    protected abstract client: IClient;

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
        return !!this._meta.sharedTables;
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


    protected abstract getSQLType(type: AttributeEnum, size: number, signed?: boolean, array?: boolean): string;

    protected getSQLTable(name: string): string {
        if (!name) {
            throw new DatabaseException("Failed to get SQL table: name is empty");
        }
        return `${this.$.quote(this.$database)}.${this.$.quote(`${this.$namespace}_${name}`)}`;
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
