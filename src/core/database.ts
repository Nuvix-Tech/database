import { EventEmitter } from "stream";
import { AttributeEnum, EventsEnum, PermissionEnum } from "./enums.js";
import { Attribute, Collection } from "@validators/schema.js";
import { Adapter } from "@adapters/base.js";
import { Cache } from "@nuvix/cache";
import { Filters } from "./types.js";

export class Database extends EventEmitter<Record<EventsEnum, any>> {
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
            size: Database.LENGTH_KEY,
            required: true,
        },
        {
            $id: "$sequence",
            type: AttributeEnum.Integer,
            size: Database.LENGTH_KEY,
            required: true,
        },
        {
            $id: "$collection",
            type: AttributeEnum.String,
            size: Database.LENGTH_KEY,
            required: true,
        },
        {
            $id: "$tenant",
            type: AttributeEnum.Integer,
            size: 0
        },
        {
            $id: "$createdAt",
            type: AttributeEnum.Date,
            size: 0,
            signed: false,
            default: null,
            filters: ["datetime"],
        },
        {
            $id: "$updatedAt",
            type: AttributeEnum.Date,
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
        $id: Database.METADATA,
        $collection: Database.METADATA,
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

    constructor(adapter: Adapter, cache: Cache, options: DatabaseOptions = {}) {
        super();
        this.adapter = adapter;
        this.cache = cache;
        this.instanceFilters = options.filters || {};
    }

}


export type DatabaseOptions = {
    tenant?: number;
    filters?: Filters;
};
