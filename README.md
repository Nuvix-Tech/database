# Nuvix Database Library

A powerful and modern TypeScript database library inspired by cutting-edge technologies and best practices.

## Features

- TypeScript-first approach with full type safety
- Adapter-based design supporting multiple database backends
- Document-oriented API with flexible querying capabilities
- Built-in caching for improved performance
- Comprehensive permission system
- Transaction support

## Installation

```bash
npm install @nuvix/database
```

## Usage

```typescript
import { Database } from "@nuvix/database";
import { PostgreDB } from "@nuvix/database/src/adapter/postgre";
import { Cache, Redis } from "@nuvix/cache";

// Create a database adapter
const adapter = new PostgreDB({
    connection: {
        connectionString: "postgres://user:password@localhost:5432/mydb",
        ssl: {
            rejectUnauthorized: false,
        },
    },
    schema: "public",
});

// Initialize the adapter
adapter.init();

// Create a cache instance
const cache = new Cache(new Redis({}));

// Create the database instance
const db = new Database(adapter, cache, {
    logger: true,
});

// Now you can use the database
```

## Testing

This library includes comprehensive tests for both core functionality and specific adapter implementations.

### Running Tests

```bash
# Run core tests only
npm run test

# Run PostgreSQL adapter tests
npm run test:pg

# Run all tests
npm run test:all

# Run tests with specific pattern
npm run test -- --testPathPattern=your-pattern
```

### Test Configuration

Configure test settings by setting environment variables:

```bash
# Enable PostgreSQL tests
PG_TEST_CONNECTION=true npm run test:pg
```

## License

BSD-3-Clause
