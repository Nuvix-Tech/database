# @nuvix/db

[![npm version](https://img.shields.io/npm/v/@nuvix/db.svg)](https://www.npmjs.com/package/@nuvix/db)
[![TypeScript](https://img.shields.io/badge/TypeScript-5.8+-blue.svg)](https://www.typescriptlang.org/)
[![License](https://img.shields.io/badge/License-Proprietary_License-blue.svg)](LICENSE)
[![Build Status](https://img.shields.io/github/workflow/status/Nuvix-Tech/database/CI)](https://github.com/Nuvix-Tech/database/actions)

A modular and performant database library for Nuvix, with internal complexity abstracted from developers. Built with TypeScript, this library provides a high-level interface for PostgreSQL databases with support for relationships, validation, caching, and more.

## Features

🚀 **High Performance** - Optimized queries and connection pooling  
🔒 **Type Safe** - Full TypeScript support with generated types  
📊 **Relationships** - OneToOne, OneToMany, ManyToOne, and ManyToMany relationships  
🛡️ **Security** - Built-in authorization and document-level permissions  
✅ **Validation** - Comprehensive data validation and structure checking  
🎯 **Query Builder** - Fluent query interface with filters, sorting, and pagination  
📇 **Indexing** - Support for key, unique, and fulltext indexes  
🔄 **Transactions** - ACID transaction support  
💾 **Caching** - Integrated caching layer for improved performance  
🏢 **Multi-tenancy** - Built-in support for shared tables and tenant isolation  
📝 **Migrations** - Schema migration support  

## Installation

```bash
npm install @nuvix/db
# or
yarn add @nuvix/db
# or
bun add @nuvix/db
```

## Quick Start

```typescript
import { Database, Adapter, Doc, AttributeEnum, Permission, Role } from '@nuvix/db';
import { Memory } from '@nuvix/cache';

// Create database adapter
const adapter = new Adapter({
  connectionString: 'postgres://user:pass@localhost:5432/mydb'
});

// Initialize database
const db = new Database(adapter, new Memory());

// Create database schema
await db.create();

// Create a collection
await db.createCollection({
  id: 'users',
  attributes: [
    new Doc({
      $id: 'name',
      key: 'name',
      type: AttributeEnum.String,
      size: 100,
      required: true
    }),
    new Doc({
      $id: 'email',
      key: 'email', 
      type: AttributeEnum.String,
      size: 255,
      required: true
    }),
    new Doc({
      $id: 'age',
      key: 'age',
      type: AttributeEnum.Integer,
      size: 4
    })
  ],
  permissions: [Permission.create(Role.any())]
});

// Create a document
const user = await db.createDocument('users', new Doc({
  name: 'John Doe',
  email: 'john@example.com',
  age: 30,
  $permissions: [Permission.read(Role.any()).toString()]
}));

// Read a document
const retrieved = await db.getDocument('users', user.getId());

// Query documents
const users = await db.find('users', (qb) => 
  qb.equal('age', 30).limit(10)
);

// Update a document
await db.updateDocument('users', user.getId(), new Doc({
  age: 31
}));

// Delete a document
await db.deleteDocument('users', user.getId());
```

## Core Concepts

### Collections and Attributes

Collections are like tables in traditional databases. Each collection has attributes that define the structure of documents:

```typescript
// Define collection attributes
const attributes = [
  new Doc({
    $id: 'title',
    key: 'title',
    type: AttributeEnum.String,
    size: 200,
    required: true
  }),
  new Doc({
    $id: 'content',
    key: 'content', 
    type: AttributeEnum.String,
    size: 5000
  }),
  new Doc({
    $id: 'published',
    key: 'published',
    type: AttributeEnum.Boolean,
    default: false
  }),
  new Doc({
    $id: 'tags',
    key: 'tags',
    type: AttributeEnum.String,
    size: 50,
    array: true // Array of strings
  })
];
```

### Supported Attribute Types

- `AttributeEnum.String` - Text with specified size limit
- `AttributeEnum.Integer` - Integer numbers with optional size
- `AttributeEnum.Float` - Floating point numbers
- `AttributeEnum.Boolean` - True/false values
- `AttributeEnum.Json` - JSON objects
- `AttributeEnum.Uuid` - UUID strings
- `AttributeEnum.Timestamptz` - Timestamps with timezone
- `AttributeEnum.Relationship` - References to other documents

### Relationships

Create relationships between collections:

```typescript
// One-to-Many: User has many Posts
await db.createRelationship({
  collectionId: 'users',
  relatedCollectionId: 'posts', 
  type: RelationEnum.OneToMany,
  id: 'posts',
  twoWay: true,
  twoWayKey: 'author'
});

// Many-to-Many: Posts have many Tags
await db.createRelationship({
  collectionId: 'posts',
  relatedCollectionId: 'tags',
  type: RelationEnum.ManyToMany,
  id: 'tags',
  twoWay: true,
  twoWayKey: 'posts'
});
```

### Querying

Use the fluent query builder for complex queries:

```typescript
// Simple queries
const users = await db.find('users', (qb) => 
  qb.equal('status', 'active')
    .greaterThan('age', 18)
    .limit(50)
    .offset(0)
    .orderBy('name', 'ASC')
);

// Complex queries with multiple conditions
const posts = await db.find('posts', (qb) =>
  qb.equal('published', true)
    .search('title', 'typescript')
    .between('created_at', '2024-01-01', '2024-12-31')
    .contains('tags', ['tutorial', 'guide'])
    .populate(['author', 'comments'])
    .select(['title', 'content', 'author.name'])
);

// Using Query objects directly
const results = await db.find('users', [
  Query.equal('status', ['active']),
  Query.greaterThan('age', 18),
  Query.limit(25),
  Query.orderBy('name')
]);
```

### Indexing

Create indexes for better query performance:

```typescript
// Key index for faster lookups
await db.createIndex('users', 'idx_email', IndexEnum.Key, ['email']);

// Unique index to enforce uniqueness
await db.createIndex('users', 'idx_username', IndexEnum.Unique, ['username']);

// Fulltext index for search
await db.createIndex('posts', 'idx_content', IndexEnum.FullText, ['title', 'content']);

// Composite index
await db.createIndex('posts', 'idx_author_date', IndexEnum.Key, ['author', 'created_at']);
```

### Permissions and Security

Control access with role-based permissions:

```typescript
// Collection-level permissions
await db.createCollection({
  id: 'posts',
  attributes: [...],
  permissions: [
    Permission.create(Role.user()),
    Permission.read(Role.any()),
    Permission.update(Role.user()),
    Permission.delete(Role.user())
  ],
  documentSecurity: true // Enable document-level permissions
});

// Document-level permissions
await db.createDocument('posts', new Doc({
  title: 'My Post',
  content: 'Content here...',
  $permissions: [
    Permission.read(Role.any()).toString(),
    Permission.update(Role.user('user123')).toString(),
    Permission.delete(Role.user('user123')).toString()
  ]
}));
```

## Advanced Usage

### Transactions

Ensure data consistency with transactions:

```typescript
await db.withTransaction(async () => {
  const user = await db.createDocument('users', userData);
  const profile = await db.createDocument('profiles', {
    userId: user.getId(),
    ...profileData
  });
  // Both operations succeed or both fail
});
```

### Multi-tenancy

Support multiple tenants in shared infrastructure:

```typescript
// Configure adapter for shared tables
adapter.setMeta({
  sharedTables: true,
  tenantId: 123,
  namespace: 'tenant_app'
});

// All operations will be scoped to the tenant
const users = await db.find('users'); // Only returns tenant 123's users
```

### Caching

Leverage built-in caching for better performance:

```typescript
// Cache is automatically managed
const user = await db.getDocument('users', 'user123'); // Fetches from DB
const userAgain = await db.getDocument('users', 'user123'); // Returns from cache

// Manual cache control
await db.purgeCachedDocument('users', 'user123');
await db.purgeCachedCollection('users');
```

### Event Handling

Listen to database events:

```typescript
db.on(EventsEnum.DocumentCreate, (document) => {
  console.log('Document created:', document.getId());
});

db.on(EventsEnum.CollectionCreate, (collection) => {
  console.log('Collection created:', collection.getId());
});
```

## Project Structure

```
src/
├── adapters/          # Database adapters (PostgreSQL)
│   ├── adapter.ts     # Main adapter implementation
│   ├── base.ts        # Base adapter class
│   ├── postgres.ts    # PostgreSQL client wrapper
│   └── types.ts       # Adapter types
├── core/              # Core database functionality
│   ├── database.ts    # Main Database class
│   ├── doc.ts         # Document class
│   ├── query.ts       # Query building
│   ├── cache.ts       # Caching layer
│   └── enums.ts       # Type enums
├── errors/            # Custom error classes
├── utils/             # Utility functions
│   ├── authorization.ts # Permission handling
│   ├── id.ts          # ID generation
│   ├── permission.ts  # Permission utilities
│   └── query-builder.ts # Query builder
└── validators/        # Data validation
    ├── schema.ts      # Schema validation
    ├── queries/       # Query validation
    └── permissions.ts # Permission validation
```

## Development

### Prerequisites

- Node.js 18.17 or later
- PostgreSQL 12 or later
- Bun (recommended) or npm/yarn

### Setup

```bash
# Clone the repository
git clone https://github.com/Nuvix-Tech/database.git
cd database

# Install dependencies
bun install

# Set up environment variables
cp .env.example .env
# Edit .env with your database configuration

# Run tests
bun test

# Build the library
bun run build

# Type checking
bun run typecheck

# Lint code
bun run lint
```

### Running Tests

```bash
# Run all tests
bun test

# Watch mode
bun run test:watch

# Test specific file
bun test tests/database.basic.test.ts
```

### Environment Variables

```bash
# PostgreSQL connection string
PG_URL=postgres://postgres:postgres@localhost:5432/test_db
```

## Configuration

### Database Adapter Options

```typescript
const adapter = new Adapter({
  // PostgreSQL connection config
  host: 'localhost',
  port: 5432,
  database: 'myapp',
  user: 'username',
  password: 'password',
  
  // Or use connection string
  connectionString: 'postgres://user:pass@host:port/db',
  
  // Connection pool settings
  max: 20,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 2000
});

// Set metadata for multi-tenancy
adapter.setMeta({
  database: 'myapp',
  schema: 'public',
  sharedTables: false,
  tenantId: undefined,
  tenantPerDocument: false,
  namespace: 'default'
});
```

### Cache Configuration

```typescript
import { Memory, Redis } from '@nuvix/cache';

// In-memory cache (development)
const cache = new Memory();

// Redis cache (production)
const cache = new Redis({
  host: 'localhost',
  port: 6379,
  password: 'redis-password'
});
```

## API Reference

### Database Class Methods

#### Collection Management
- `create(database?)` - Create database schema
- `createCollection(options)` - Create a new collection
- `getCollection(id)` - Get collection metadata
- `listCollections(limit?, offset?)` - List all collections
- `updateCollection(options)` - Update collection permissions
- `deleteCollection(id)` - Delete a collection

#### Document Operations
- `createDocument(collectionId, document)` - Create a document
- `createDocuments(collectionId, documents)` - Create multiple documents
- `getDocument(collectionId, id, query?)` - Get document by ID
- `updateDocument(collectionId, id, updates)` - Update a document
- `updateDocuments(collectionId, updates, query?)` - Update multiple documents
- `deleteDocument(collectionId, id)` - Delete a document
- `deleteDocuments(collectionId, query?)` - Delete multiple documents
- `find(collectionId, query?)` - Query documents

#### Attribute Management
- `createAttribute(collectionId, attribute)` - Add attribute to collection
- `updateAttribute(collectionId, id, options)` - Update attribute properties
- `deleteAttribute(collectionId, id)` - Remove attribute from collection
- `renameAttribute(collectionId, oldName, newName)` - Rename an attribute

#### Relationship Management
- `createRelationship(options)` - Create relationship between collections
- `updateRelationship(options)` - Update existing relationship
- `deleteRelationship(collectionId, id)` - Delete a relationship

#### Index Management
- `createIndex(collectionId, id, type, attributes)` - Create an index
- `deleteIndex(collectionId, id)` - Delete an index
- `renameIndex(collectionId, oldName, newName)` - Rename an index

## Contributing

We welcome contributions! Please follow these guidelines:

1. **Fork** the repository
2. **Create** a feature branch (`git checkout -b feature/amazing-feature`)
3. **Write** tests for your changes
4. **Ensure** all tests pass (`bun test`)
5. **Commit** your changes (`git commit -m 'Add amazing feature'`)
6. **Push** to the branch (`git push origin feature/amazing-feature`)
7. **Open** a Pull Request

### Code Style

- Use TypeScript with strict type checking
- Follow existing code formatting (Prettier)
- Write comprehensive tests for new features
- Update documentation for API changes

### Testing

All contributions must include tests. We use Vitest for testing:

```typescript
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { createTestDb } from './helpers.js';

describe('Feature', () => {
  const db = createTestDb();
  
  beforeAll(async () => {
    await db.create();
  });
  
  afterAll(async () => {
    await db.getAdapter().$client.disconnect();
  });
  
  it('should work correctly', async () => {
    // Test implementation
    expect(result).toBe(expected);
  });
});
```

## License

This project is licensed under the Proprietary License - see the [LICENSE](LICENSE) file for details.

## Support

- 📖 [Documentation](https://docs.nuvix.in/database)
- 🐛 [Issue Tracker](https://github.com/Nuvix-Tech/database/issues)
- 💬 [Discussions](https://github.com/Nuvix-Tech/database/discussions)
- 📧 [Email Support](mailto:support@nuvix.in)

---

Built with ❤️ by the Nuvix team
