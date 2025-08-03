
export enum AttributeEnum {
    String = "string",
    Integer = "integer",
    Float = "float",
    Boolean = "boolean",
    /**@deprecated use timestamptz*/
    Datetime = "timestamptz",
    Timestamptz = "timestamptz",
    Json = "jsonb",
    Relationship = "relationship",
    Virtual = "virtual",
    /**@deprecated Not Implemented */
    Uuid = "uuid",
}

export enum PermissionEnum {
    Create = "create",
    Read = "read",
    Update = "update",
    Delete = "delete",
    Write = 'write',
}

export enum RelationEnum {
    OneToOne = "oneToOne",
    OneToMany = "oneToMany",
    ManyToOne = "manyToOne",
    ManyToMany = "manyToMany",
}

export enum RelationSideEnum {
    Parent = "parent",
    Child = "child",
}

export enum IndexEnum {
    Unique = "unique",
    Key = 'key',
    FullText = "fulltext",
    Spatial = "spatial",
}

export enum EventsEnum {
    All = "*",

    DatabaseList = "database_list",
    DatabaseCreate = "database_create",
    DatabaseDelete = "database_delete",

    CollectionList = "collection_list",
    CollectionCreate = "collection_create",
    CollectionUpdate = "collection_update",
    CollectionRead = "collection_read",
    CollectionDelete = "collection_delete",

    DocumentFind = "document_find",
    DocumentPurge = "document_purge",
    DocumentCreate = "document_create",
    DocumentsCreate = "documents_create",
    DocumentRead = "document_read",
    DocumentUpdate = "document_update",
    DocumentsUpdate = "documents_update",
    DocumentsUpsert = "documents_upsert",
    DocumentDelete = "document_delete",
    DocumentsDelete = "documents_delete",
    DocumentCount = "document_count",
    DocumentSum = "document_sum",
    DocumentIncrease = "document_increase",
    DocumentDecrease = "document_decrease",

    PermissionsCreate = "permissions_create",
    PermissionsRead = "permissions_read",
    PermissionsDelete = "permissions_delete",

    AttributeCreate = "attribute_create",
    AttributesCreate = "attributes_create",
    AttributeUpdate = "attribute_update",
    AttributeDelete = "attribute_delete",

    IndexRename = "index_rename",
    IndexCreate = "index_create",
    IndexDelete = "index_delete",
}

export enum CursorEnum {
    After = 'after',
    Before = 'before'
}

export enum OrderEnum {
    Asc = 'ASC',
    Desc = 'DESC'
}
