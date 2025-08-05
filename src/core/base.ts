import { Attribute, Collection } from "@validators/schema.js";
import { Emitter, EmitterEventMap } from "./emitter.js";
import { AttributeEnum, EventsEnum, PermissionEnum } from "./enums.js";
import { Cache } from "@nuvix/cache";
import { Filter, Filters } from "./types.js";
import { Adapter, Meta } from "@adapters/base.js";
import { filters } from "@utils/filters.js";
import { Doc } from "./doc.js";
import { DatabaseException, NotFoundException } from "@errors/index.js";

export abstract class Base<T extends EmitterEventMap = EmitterEventMap> extends Emitter<T> {
    public static METADATA = '_metadata' as const;

    public static readonly INT_MAX = 2147483647;
    public static readonly BIG_INT_MAX = Number.MAX_SAFE_INTEGER;
    public static readonly DOUBLE_MAX = Number.MAX_VALUE
    public static readonly ARRAY_INDEX_LENGTH = 255;
    public static readonly RELATION_MAX_DEPTH = 3;
    public static readonly LENGTH_KEY = 255;
    public static readonly TTL = 60 * 60 * 24; // 24 hours
    public static readonly INSERT_BATCH_SIZE = 1000;
    public static readonly DELETE_BATCH_SIZE = 1000;

    public static readonly INTERNAL_ATTRIBUTES: Attribute[] = [
        {
            $id: "$id",
            key: "$id",
            type: AttributeEnum.String,
            size: Base.LENGTH_KEY,
            required: true,
        },
        {
            $id: "$sequence",
            key: "$sequence",
            type: AttributeEnum.Integer,
            size: 8,
        },
        {
            $id: "$collection",
            key: "$collection",
            type: AttributeEnum.String,
            size: Base.LENGTH_KEY,
            required: true,
        },
        {
            $id: "$tenant",
            key: "$tenant",
            type: AttributeEnum.Integer,
            size: 8
        },
        {
            $id: "$createdAt",
            key: "$createdAt",
            type: AttributeEnum.Timestamptz,
            default: null,
        },
        {
            $id: "$updatedAt",
            key: "$updatedAt",
            type: AttributeEnum.Timestamptz,
            default: null,
        },
        {
            $id: "$permissions",
            key: "$permissions",
            type: AttributeEnum.String,
            size: 255,
            array: true,
        },
    ];
    public static readonly INTERNAL_ATTRIBUTE_KEYS = [
        '_uid',
        '_createdAt',
        '_updatedAt',
        '_permissions',
    ];
    public static readonly INTERNAL_INDEXES: string[] = [
        '_id',
        '_uid',
        '_createdAt',
        '_updatedAt',
        '_permissions_id',
        '_permissions',
    ];
    protected static readonly COLLECTION: Collection = {
        $id: Base.METADATA,
        $collection: Base.METADATA,
        name: "collections",
        attributes: [
            {
                $id: "name",
                key: "name",
                type: AttributeEnum.String,
                size: 256,
                required: true,
            },
            {
                $id: "attributes",
                key: "attributes",
                type: AttributeEnum.Json,
            },
            {
                $id: "indexes",
                key: "indexes",
                type: AttributeEnum.Json,
            },
            {
                $id: "documentSecurity",
                key: "documentSecurity",
                type: AttributeEnum.Boolean,
                required: true,
            },
        ],
        documentSecurity: false,
    };

    public static readonly PERMISSIONS: PermissionEnum[] = [
        PermissionEnum.Create,
        PermissionEnum.Read,
        PermissionEnum.Update,
        PermissionEnum.Delete,
    ];

    protected readonly adapter: Adapter;
    protected readonly cache: Cache;

    protected static filters: Filters = {};
    protected readonly instanceFilters: Filters;
    protected timestamp?: Date;
    protected filter: boolean = true;
    protected validate: boolean = true;
    protected preserveDates: boolean = false;
    protected maxQueryValues: number = 100;
    protected globalCollections: Record<string, boolean> = {}

    constructor(adapter: Adapter, cache: Cache, options: Options = {}) {
        super();
        this.adapter = adapter;
        this.cache = cache;
        this.instanceFilters = options.filters || {};

        for (const [filterName, FilterValue] of Object.entries(filters)) {
            Base.filters[filterName] = FilterValue as Filter;
        }
    }

    public addFilter(name: string, filter: Filter): this {
        if (this.instanceFilters[name]) {
            throw new Error(`Filter with name "${name}" already exists.`);
        }
        this.instanceFilters[name] = filter;
        return this;
    }

    public static addFilter(name: string, filter: Filter): void {
        if (Base.filters[name]) {
            throw new Error(`Filter with name "${name}" already exists.`);
        }
        Base.filters[name] = filter;
    }

    public getFilters(): Filters {
        return { ...Base.filters, ...this.instanceFilters };
    }

    public getAdapter(): Adapter {
        return this.adapter;
    }

    public enableFilters(): this {
        this.filter = true;
        return this;
    }

    public disableFilters(): this {
        this.filter = false;
        return this;
    }

    public enableValidation(): this {
        this.validate = true;
        return this;
    }

    public disableValidation(): this {
        this.validate = false;
        return this;
    }

    public setMeta(meta: Partial<Meta>): this {
        this.adapter.setMeta(meta);
        return this;
    }

    public get database() {
        return this.adapter.$database;
    }

    public get sharedTables(): boolean {
        return this.adapter.$sharedTables;
    }

    public get migrating(): boolean {
        return false; // TODO: ----
    }

    public get tenantId(): number | undefined {
        return this.adapter.$tenantId;
    }

    public get tenantPerDocument(): boolean {
        return this.adapter.$tenantPerDocument;
    }

    public get namespace(): string {
        return this.adapter.$namespace;
    }

    public get metadata() {
        return this.adapter.$metadata;
    }

    public before(event: EventsEnum, name: string, callback?: (query: string) => string) {
        this.adapter.before(event, name, callback);
        return this;
    }

    public async withRequestTimestamp<T>(
        requestTimestamp: Date | null,
        callback: Callback<T>,
    ): Promise<T> {
        const previous = this.timestamp;
        this.timestamp = requestTimestamp ?? undefined;
        try {
            return await callback();
        } finally {
            this.timestamp = previous;
        }
    }

    public async skipFilters<T>(callback: Callback<T>): Promise<T> {
        const initial = this.filter;
        this.disableFilters();

        try {
            return await callback();
        } finally {
            this.filter = initial;
        }
    }

    public async skipValidation<T>(callback: Callback<T>): Promise<T> {
        const initial = this.validate;
        this.disableValidation();

        try {
            return await callback();
        } finally {
            this.validate = initial;
        }
    }

    public async withTenant<T>(
        tenantId: number | null,
        callback: Callback<T>,
    ): Promise<T> {
        const previous = this.adapter.$tenantId;
        this.adapter.setMeta({ tenantId: tenantId ?? undefined });

        try {
            return await callback();
        } finally {
            this.adapter.setMeta({ tenantId: previous });
        }
    }

    public async withPreserveDates<T>(callback: Callback<T>): Promise<T> {
        const previous = this.preserveDates;
        this.preserveDates = true;

        try {
            return await callback();
        } finally {
            this.preserveDates = previous;
        }
    }

    public get withTransaction() {
        return this.adapter.$client.transaction
    }

    public get ping() {
        return this.adapter.$client.ping
    }

    protected cast<T extends Record<string, any>>(collection: Doc<Collection>, document: Doc<T>): Doc<T> {
        if (this.adapter.$supportForCasting) {
            return document;
        }

        const attributes: (Attribute | Doc<Attribute>)[] = collection.get('attributes') ?? [];
        for (const attribute of Base.INTERNAL_ATTRIBUTES) {
            attributes.push(attribute);
        }

        for (const attr of attributes) {
            const attribute = attr instanceof Doc ? attr.toObject() : attr;
            const key = attribute.$id ?? '';
            const type = attribute.type ?? '';
            const array = attribute.array ?? false;
            const value = document.get(key, null);

            if (value === null || value === undefined) {
                continue;
            }

            let processedValue: any;
            if (array) {
                processedValue = typeof value === 'string'
                    ? JSON.parse(value)
                    : value;
            } else {
                processedValue = [value];
            }

            for (let index = 0; index < processedValue.length; index++) {
                let node = processedValue[index];

                switch (type) {
                    case AttributeEnum.Boolean:
                        node = Boolean(node);
                        break;
                    case AttributeEnum.Integer:
                        node = parseInt(node, 10);
                        break;
                    case AttributeEnum.Float:
                        node = parseFloat(node);
                        break;
                    default:
                        break;
                }

                processedValue[index] = node;
            }

            document.set(key, array ? processedValue : processedValue[0]);
        }
        return document;
    }

    protected async encode<T extends Record<string, any>>(collection: Doc<Collection>, document: Doc<T>): Promise<Doc<T>> {
        const attributes: (Attribute | Doc<Attribute>)[] = collection.get('attributes') ?? [];
        const internalDateAttributes = ['$createdAt', '$updatedAt'];

        for (const attribute of this.getInternalAttributes()) {
            attributes.push(attribute);
        }

        for (const attr of attributes) {
            const attribute = attr instanceof Doc ? attr.toObject() : attr;
            const key = attribute.$id ?? '';
            const array = attribute.array ?? false;
            const defaultValue = attribute.default ?? null;
            const attributeFilters = attribute.filters ?? [];
            let value: any = document.get(key);

            if (attribute.type === AttributeEnum.Virtual) {
                document.delete(key)
                continue;
            }

            if (internalDateAttributes.includes(key) && typeof value === 'string' && value === '') {
                document.set(key, null);
                continue;
            }

            if (key === '$permissions') {
                if (!value) {
                    document.set('$permissions', []);
                }
                continue;
            }

            // Continue on optional param with no default
            if (value === null && defaultValue === null) {
                continue;
            }

            // Assign default only if no value provided
            if (value === null && defaultValue !== null) {
                value = array ? defaultValue : [defaultValue];
            } else {
                value = array ? value : [value];
            }

            for (let index = 0; index < value.length; index++) {
                let node = value[index];
                if (node !== null) {
                    for (const filter of attributeFilters) {
                        node = await this.encodeAttribute(filter, node, document as unknown as Doc);
                    }
                    value[index] = node;
                }
            }

            if (!array) {
                value = value[0];
            }
            if (attribute.type === AttributeEnum.Json && typeof value === 'object') {
                value = JSON.stringify(value);
            }
            document.set(key, value);
        }

        return document;
    }

    protected async decode<T extends Record<string, any>>(
        collection: Doc<Collection>,
        document: Doc<T>,
        selections: string[] = []
    ): Promise<Doc<T>> {
        const collectionAttributes: (Attribute | Doc<Attribute>)[] = collection.get('attributes') ?? [];
        const internalAttributes = this.getInternalAttributes();

        const preparedAttributes: Attribute[] = [
            ...collectionAttributes.map(attr => attr instanceof Doc ? attr.toObject() : attr),
            ...internalAttributes.map(attr => attr instanceof Doc ? attr.toObject() : attr)
        ];

        for (const attribute of preparedAttributes) {
            const originalKey = attribute.$id;
            if (!originalKey || attribute.type !== AttributeEnum.Relationship) continue;

            const sanitizedKey = this.adapter.sanitize(originalKey);
            if (originalKey !== sanitizedKey && document.has(sanitizedKey)) {
                const valueFromSanitized = document.get(sanitizedKey);

                if (!document.has(originalKey) || document.get(originalKey) === undefined || document.get(originalKey) === null) {
                    document.set(originalKey, valueFromSanitized);
                }
                document.delete(sanitizedKey);
            }
        }

        for (const attribute of preparedAttributes) {
            const key = attribute.$id;
            if (!key || attribute.type === AttributeEnum.Relationship) continue;

            const isArrayAttribute = attribute.array ?? false;
            const attributeFilters = attribute.filters ?? [];

            let value: any = document.get(key);
            let valuesToProcess: any[];

            if (value === null || value === undefined) {
                valuesToProcess = [];
            } else if (Array.isArray(value)) {
                valuesToProcess = value;
            } else {
                valuesToProcess = [value];
            }

            const processedValues: any[] = [];
            for (let index = 0; index < valuesToProcess.length; index++) {
                let node = valuesToProcess[index];
                for (const filter of attributeFilters.slice().reverse()) {
                    node = await this.decodeAttribute(filter, node, document as unknown as Doc, key);
                }
                processedValues[index] = node;
            }

            const isSelected = selections.length === 0 || selections.includes(key) || selections.includes('*');

            if (isSelected) {
                document.set(key, isArrayAttribute ? processedValues : (processedValues[0] ?? null));
            } else {
                document.delete(key);
            }
        }
        return document;
    }

    private async encodeAttribute(filter: string, value: any, document: Doc): Promise<any> {
        const allFilters = this.getFilters();

        if (!allFilters[filter]) {
            throw new NotFoundException(`Filter: ${filter} not found`);
        }

        try {
            if (this.instanceFilters[filter]) {
                value = this.instanceFilters[filter].encode(value, document, this as any);
            } else {
                value = Base.filters[filter]!.encode(value, document, this as any);
            }
            if (value instanceof Promise) {
                value = await value;
            }
        } catch (error) {
            throw new DatabaseException(error instanceof Error ? error.message : String(error));
        }

        return value;
    }

    private async decodeAttribute(filter: string, value: any, document: Doc, attribute: string): Promise<any> {
        if (!this.filter) {
            return value;
        }

        const allFilters = this.getFilters();
        if (!allFilters[filter]) {
            throw new NotFoundException(`Filter "${filter}" not found for attribute "${attribute}"`);
        }

        try {
            if (this.instanceFilters[filter]) {
                value = this.instanceFilters[filter].decode(value, document, this as any);
            } else {
                value = Base.filters[filter]!.decode(value, document, this as any);
            }
            if (value instanceof Promise) {
                value = await value;
            }
        } catch (error) {
            throw new DatabaseException(error instanceof Error ? error.message : String(error));
        }

        return value;
    }

    public getInternalAttributes(): Attribute[] {
        let attributes = Base.INTERNAL_ATTRIBUTES;

        if (!this.sharedTables) {
            attributes = Base.INTERNAL_ATTRIBUTES.filter(attribute =>
                attribute.$id !== '$tenant'
            );
        }

        return attributes;
    }
}

type Options = {
    tenant?: number;
    filters?: Filters;
};

type Callback<T> = () => Promise<T> | T;
