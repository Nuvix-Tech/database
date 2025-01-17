


export class Database {
  public static VAR_STRING = 'string';
  // Simple Types
  public static VAR_INTEGER = 'integer';
  public static VAR_FLOAT = 'double';
  public static VAR_BOOLEAN = 'boolean';
  public static VAR_DATETIME = 'datetime';

  public static INT_MAX = 2147483647;
  public static BIG_INT_MAX = Number.MAX_SAFE_INTEGER;
  public static DOUBLE_MAX = Number.MAX_VALUE;

  // Relationship Types
  public static VAR_RELATIONSHIP = 'relationship';

  // Index Types
  public static INDEX_KEY = 'key';
  public static INDEX_FULLTEXT = 'fulltext';
  public static INDEX_UNIQUE = 'unique';
  public static INDEX_SPATIAL = 'spatial';
  public static ARRAY_INDEX_LENGTH = 255;

  // Relation Types
  public static RELATION_ONE_TO_ONE = 'oneToOne';
  public static RELATION_ONE_TO_MANY = 'oneToMany';
  public static RELATION_MANY_TO_ONE = 'manyToOne';
  public static RELATION_MANY_TO_MANY = 'manyToMany';

  // Relation Actions
  public static RELATION_MUTATE_CASCADE = 'cascade';
  public static RELATION_MUTATE_RESTRICT = 'restrict';
  public static RELATION_MUTATE_SET_NULL = 'setNull';

  // Relation Sides
  public static RELATION_SIDE_PARENT = 'parent';
  public static RELATION_SIDE_CHILD = 'child';

  public static RELATION_MAX_DEPTH = 3;

  // Orders
  public static ORDER_ASC = 'ASC';
  public static ORDER_DESC = 'DESC';

  // Permissions
  public static PERMISSION_CREATE = 'create';
  public static PERMISSION_READ = 'read';
  public static PERMISSION_UPDATE = 'update';
  public static PERMISSION_DELETE = 'delete';

  // Aggregate permissions
  public static PERMISSION_WRITE = 'write';

  public static PERMISSIONS = [
    this.PERMISSION_CREATE,
    this.PERMISSION_READ,
    this.PERMISSION_UPDATE,
    this.PERMISSION_DELETE,
  ];

  // Collections
  public static METADATA = '_metadata';

  // Cursor
  public static CURSOR_BEFORE = 'before';
  public static CURSOR_AFTER = 'after';

  // Lengths
  public static LENGTH_KEY = 255;

  // Cache
  public static TTL = 60 * 60 * 24; // 24 hours

  // Events
  public static EVENT_ALL = '*';

  public static EVENT_DATABASE_LIST = 'database_list';
  public static EVENT_DATABASE_CREATE = 'database_create';
  public static EVENT_DATABASE_DELETE = 'database_delete';

  public static EVENT_COLLECTION_LIST = 'collection_list';
  public static EVENT_COLLECTION_CREATE = 'collection_create';
  public static EVENT_COLLECTION_UPDATE = 'collection_update';
  public static EVENT_COLLECTION_READ = 'collection_read';
  public static EVENT_COLLECTION_DELETE = 'collection_delete';

  public static EVENT_DOCUMENT_FIND = 'document_find';
  public static EVENT_DOCUMENT_CREATE = 'document_create';
  public static EVENT_DOCUMENTS_CREATE = 'documents_create';
  public static EVENT_DOCUMENTS_DELETE = 'documents_delete';
  public static EVENT_DOCUMENT_READ = 'document_read';
  public static EVENT_DOCUMENT_UPDATE = 'document_update';
  public static EVENT_DOCUMENTS_UPDATE = 'documents_update';
  public static EVENT_DOCUMENT_DELETE = 'document_delete';
  public static EVENT_DOCUMENT_COUNT = 'document_count';
  public static EVENT_DOCUMENT_SUM = 'document_sum';
  public static EVENT_DOCUMENT_INCREASE = 'document_increase';
  public static EVENT_DOCUMENT_DECREASE = 'document_decrease';

  public static EVENT_PERMISSIONS_CREATE = 'permissions_create';
  public static EVENT_PERMISSIONS_READ = 'permissions_read';
  public static EVENT_PERMISSIONS_DELETE = 'permissions_delete';

  public static EVENT_ATTRIBUTE_CREATE = 'attribute_create';
  public static EVENT_ATTRIBUTE_UPDATE = 'attribute_update';
  public static EVENT_ATTRIBUTE_DELETE = 'attribute_delete';

  public static EVENT_INDEX_RENAME = 'index_rename';
  public static EVENT_INDEX_CREATE = 'index_create';
  public static EVENT_INDEX_DELETE = 'index_delete';

  public static INSERT_BATCH_SIZE = 100;
  public static DELETE_BATCH_SIZE = 100;

  // Internal attributes
  // TODO: ---
  public static INTERNAL_ATTRIBUTES = [
    {
      'id': '$id',
      'type': Database.VAR_STRING,
      'size': Database.LENGTH_KEY,
      'required': true,
      'signed': true,
      'array': false,
      'filters': [],
    },
    {
      'id': '$internalId',
      'type': Database.VAR_STRING,
      'size': Database.LENGTH_KEY,
      'required': true,
      'signed': true,
      'array': false,
      'filters': [],
    },
    {
      'id': '$collection',
      'type': Database.VAR_STRING,
      'size': Database.LENGTH_KEY,
      'required': true,
      'signed': true,
      'array': false,
      'filters': [],
    },
    {
      'id': '$tenant',
      'type': Database.VAR_INTEGER,
      'size': 0,
      'required': false,
      'default': null,
      'signed': true,
      'array': false,
      'filters': [],
    },
    {
      'id': '$createdAt',
      'type': Database.VAR_DATETIME,
      'format': '',
      'size': 0,
      'signed': false,
      'required': false,
      'default': null,
      'array': false,
      'filters': ['datetime'],
    },
    {
      'id': '$updatedAt',
      'type': Database.VAR_DATETIME,
      'format': '',
      'size': 0,
      'signed': false,
      'required': false,
      'default': null,
      'array': false,
      'filters': ['datetime'],
    },
    {
      '$id': '$permissions',
      'type': Database.VAR_STRING,
      'size': 1000000,
      'signed': true,
      'required': false,
      'default': [],
      'array': false,
      'filters': ['json'],
    },
  ];

  public static INTERNAL_INDEXES = [
    '_id',
    '_uid',
    '_createdAt',
    '_updatedAt',
    '_permissions_id',
    '_permissions',
  ];

  /**
   * Parent Collection
   * Defines the structure for both system and custom collections
   *
   * @var array<string, mixed>
   */
  protected static COLLECTION = {
    '$id': Database.METADATA,
    '$collection': Database.METADATA,
    'name': 'collections',
    'attributes': [
      {
        '$id': 'name',
        'key': 'name',
        'type': Database.VAR_STRING,
        'size': 256,
        'required': true,
        'signed': true,
        'array': false,
        'filters': [],
      },
      {
        '$id': 'attributes',
        'key': 'attributes',
        'type': Database.VAR_STRING,
        'size': 1000000,
        'required': false,
        'signed': true,
        'array': false,
        'filters': ['json'],
      },
      {
        '$id': 'indexes',
        'key': 'indexes',
        'type': Database.VAR_STRING,
        'size': 1000000,
        'required': false,
        'signed': true,
        'array': false,
        'filters': ['json'],
      },
      {
        '$id': 'documentSecurity',
        'key': 'documentSecurity',
        'type': Database.VAR_BOOLEAN,
        'size': 0,
        'required': true,
        'signed': true,
        'array': false,
        'filters': []
      }
    ],
    'indexes': [],
  };


}