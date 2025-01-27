export class Constant {
    public static readonly VAR_STRING = "string";
    // Simple Types
    public static readonly VAR_INTEGER = "integer";
    public static readonly VAR_FLOAT = "double";
    public static readonly VAR_BOOLEAN = "boolean";
    public static readonly VAR_DATETIME = "datetime";

    public static readonly INT_MAX = 2147483647; // Maximum value for a 32-bit signed integer in MariaDB
    public static readonly BIG_INT_MAX = Number.MAX_SAFE_INTEGER;
    public static readonly DOUBLE_MAX = Number.MAX_VALUE;

    // Relationship Types
    public static readonly VAR_RELATIONSHIP = "relationship";

    // Virtual Types
    public static readonly VAR_VIRTUAL = "virtual";

    // Index Types
    public static readonly INDEX_KEY = "key";
    public static readonly INDEX_FULLTEXT = "fulltext";
    public static readonly INDEX_UNIQUE = "unique";
    public static readonly INDEX_SPATIAL = "spatial";
    public static readonly ARRAY_INDEX_LENGTH = 255;

    // Relation Types
    public static readonly RELATION_ONE_TO_ONE = "oneToOne";
    public static readonly RELATION_ONE_TO_MANY = "oneToMany";
    public static readonly RELATION_MANY_TO_ONE = "manyToOne";
    public static readonly RELATION_MANY_TO_MANY = "manyToMany";

    // Relation Actions
    public static readonly RELATION_MUTATE_CASCADE = "cascade";
    public static readonly RELATION_MUTATE_RESTRICT = "restrict";
    public static readonly RELATION_MUTATE_SET_NULL = "setNull";

    // Relation Sides
    public static readonly RELATION_SIDE_PARENT = "parent";
    public static readonly RELATION_SIDE_CHILD = "child";

    public static readonly RELATION_MAX_DEPTH = 3;

    // Orders
    public static readonly ORDER_ASC = "ASC";
    public static readonly ORDER_DESC = "DESC";

    // Permissions
    public static readonly PERMISSION_CREATE = "create";
    public static readonly PERMISSION_READ = "read";
    public static readonly PERMISSION_UPDATE = "update";
    public static readonly PERMISSION_DELETE = "delete";

    // Aggregate permissions
    public static readonly PERMISSION_WRITE = "write";

    public static readonly PERMISSIONS = [
        this.PERMISSION_CREATE,
        this.PERMISSION_READ,
        this.PERMISSION_UPDATE,
        this.PERMISSION_DELETE,
    ];

    // Collections
    public static readonly METADATA = "_metadata";

    // Cursor
    public static readonly CURSOR_BEFORE = "before";
    public static readonly CURSOR_AFTER = "after";

    // Lengths
    public static readonly LENGTH_KEY = 255;

    // Cache
    public static readonly TTL = 60 * 60 * 24; // 24 hours

    // Events
    public static readonly EVENT_ALL = "*";

    public static readonly EVENT_DATABASE_LIST = "database_list";
    public static readonly EVENT_DATABASE_CREATE = "database_create";
    public static readonly EVENT_DATABASE_DELETE = "database_delete";

    public static readonly EVENT_COLLECTION_LIST = "collection_list";
    public static readonly EVENT_COLLECTION_CREATE = "collection_create";
    public static readonly EVENT_COLLECTION_UPDATE = "collection_update";
    public static readonly EVENT_COLLECTION_READ = "collection_read";
    public static readonly EVENT_COLLECTION_DELETE = "collection_delete";

    public static readonly EVENT_DOCUMENT_FIND = "document_find";
    public static readonly EVENT_DOCUMENT_CREATE = "document_create";
    public static readonly EVENT_DOCUMENTS_CREATE = "documents_create";
    public static readonly EVENT_DOCUMENTS_DELETE = "documents_delete";
    public static readonly EVENT_DOCUMENT_READ = "document_read";
    public static readonly EVENT_DOCUMENT_UPDATE = "document_update";
    public static readonly EVENT_DOCUMENTS_UPDATE = "documents_update";
    public static readonly EVENT_DOCUMENT_DELETE = "document_delete";
    public static readonly EVENT_DOCUMENT_COUNT = "document_count";
    public static readonly EVENT_DOCUMENT_SUM = "document_sum";
    public static readonly EVENT_DOCUMENT_INCREASE = "document_increase";
    public static readonly EVENT_DOCUMENT_DECREASE = "document_decrease";

    public static readonly EVENT_PERMISSIONS_CREATE = "permissions_create";
    public static readonly EVENT_PERMISSIONS_READ = "permissions_read";
    public static readonly EVENT_PERMISSIONS_DELETE = "permissions_delete";

    public static readonly EVENT_ATTRIBUTE_CREATE = "attribute_create";
    public static readonly EVENT_ATTRIBUTE_UPDATE = "attribute_update";
    public static readonly EVENT_ATTRIBUTE_DELETE = "attribute_delete";

    public static readonly EVENT_INDEX_RENAME = "index_rename";
    public static readonly EVENT_INDEX_CREATE = "index_create";
    public static readonly EVENT_INDEX_DELETE = "index_delete";

    public static readonly INSERT_BATCH_SIZE = 100;
    public static readonly DELETE_BATCH_SIZE = 100;

    public static readonly INTERNAL_ATTRIBUTES = [
        {
            $id: "$id",
            type: Constant.VAR_STRING,
            size: Constant.LENGTH_KEY,
            required: true,
            signed: true,
            array: false,
            filters: [] as any[],
        },
        {
            $id: "$internalId",
            type: Constant.VAR_STRING,
            size: Constant.LENGTH_KEY,
            required: true,
            signed: true,
            array: false,
            filters: [],
        },
        {
            $id: "$collection",
            type: Constant.VAR_STRING,
            size: Constant.LENGTH_KEY,
            required: true,
            signed: true,
            array: false,
            filters: [],
        },
        {
            $id: "$tenant",
            type: Constant.VAR_INTEGER,
            size: 0,
            required: false,
            default: null,
            signed: true,
            array: false,
            filters: [],
        },
        {
            $id: "$createdAt",
            type: Constant.VAR_DATETIME,
            format: "",
            size: 0,
            signed: false,
            required: false,
            default: null,
            array: false,
            filters: ["datetime"],
        },
        {
            $id: "$updatedAt",
            type: Constant.VAR_DATETIME,
            format: "",
            size: 0,
            signed: false,
            required: false,
            default: null,
            array: false,
            filters: ["datetime"],
        },
        {
            $id: "$permissions",
            type: Constant.VAR_STRING,
            size: 1000000,
            signed: true,
            required: false,
            default: [],
            array: false,
            filters: ["json"],
        },
    ];

    public static readonly INTERNAL_INDEXES = [
        "_id",
        "_uid",
        "_createdAt",
        "_updatedAt",
        "_permissions_id",
        "_permissions",
    ];

    /**
     * Parent Collection
     * Defines the structure for both system and custom collections
     *
     * @var Object<string, mixed>
     */
    protected static COLLECTION = {
        $id: Constant.METADATA,
        $collection: Constant.METADATA,
        name: "collections",
        attributes: [
            {
                $id: "name",
                key: "name",
                type: Constant.VAR_STRING,
                size: 256,
                required: true,
                signed: true,
                array: false,
                filters: [],
            },
            {
                $id: "attributes",
                key: "attributes",
                type: Constant.VAR_STRING,
                size: 1000000,
                required: false,
                signed: true,
                array: false,
                filters: ["json"],
            },
            {
                $id: "indexes",
                key: "indexes",
                type: Constant.VAR_STRING,
                size: 1000000,
                required: false,
                signed: true,
                array: false,
                filters: ["json"],
            },
            {
                $id: "documentSecurity",
                key: "documentSecurity",
                type: Constant.VAR_BOOLEAN,
                size: 0,
                required: true,
                signed: true,
                array: false,
                filters: [],
            },
        ],
        indexes: [],
    };
}
