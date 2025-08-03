import { Attribute, Collection } from "@validators/schema.js";
import { Emitter, EmitterEventMap } from "./emitter.js";
import { AttributeEnum, PermissionEnum } from "./enums.js";
import { Cache } from "@nuvix/cache";
import { Filter, Filters } from "./types.js";
import { Adapter } from "@adapters/base.js";
import { filters } from "@utils/filters.js";

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
            type: AttributeEnum.String,
            size: Base.LENGTH_KEY,
            required: true,
        },
        {
            $id: "$sequence",
            type: AttributeEnum.Integer,
            size: Base.LENGTH_KEY,
            required: true,
        },
        {
            $id: "$collection",
            type: AttributeEnum.String,
            size: Base.LENGTH_KEY,
            required: true,
        },
        {
            $id: "$tenant",
            type: AttributeEnum.Integer,
            size: 0
        },
        {
            $id: "$createdAt",
            type: AttributeEnum.Datetime,
            size: 0,
            signed: false,
            default: null,
            filters: ["datetime"],
        },
        {
            $id: "$updatedAt",
            type: AttributeEnum.Datetime,
            size: 0,
            signed: false,
            default: null,
            filters: ["datetime"],
        },
        {
            $id: "$permissions",
            type: AttributeEnum.String,
            size: 1000000,
            default: [],
            filters: ["json"],
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
                type: AttributeEnum.String,
                size: 1000000,
                filters: ["json"],
            },
            {
                $id: "indexes",
                key: "indexes",
                type: AttributeEnum.String,
                size: 1000000,
                filters: ["json"],
            },
            {
                $id: "documentSecurity",
                key: "documentSecurity",
                type: AttributeEnum.Boolean,
                size: 0,
                required: true,
            },
        ],
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

    public getCache(): Cache {
        return this.cache;
    }


}

type Options = {
    tenant?: number;
    filters?: Filters;
};
