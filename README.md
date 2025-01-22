# Database Library

A powerful, extensible, and dynamic database library written in TypeScript. This library allows developers to define and manipulate schemas, perform efficient queries, and manage data seamlessly across different databases. It currently includes a MariaDB adapter with plans for future expansion.

## Features

- **Dynamic and Static Schema Definition**: Support for defining schemas using repository patterns (static) or structured objects (dynamic).
- **Multi-Tenancy**: Built-in support for shared tables for multi-tenant applications.
- **Query Builders**: Chainable and intuitive query builders for constructing complex queries.
- **MariaDB Adapter**: Full support for MariaDB with efficient query execution and parameter binding.
- **Customizable Adapters**: Easily extend or create new adapters for other databases.
- **Real-Time Logging**: Debug queries with parameterized outputs.
- **Extensibility**: Designed for developers to extend functionality easily.

## Installation

```bash
npm install @nuvix/database
```

## Getting Started

### Basic Usage

```typescript
import { Database, MariaDBAdapter, Query } from "@nuvix/database";

// Initialize the database
const adapter = new MariaDB({
    host: "localhost",
    user: "root",
    password: "password",
    database: "test_db",
});

await adapter.init();

const db = new Database(adapter);

// Perform a query
const documents = await db.find(
    "users",
    [Query.contains("name", ["John"]), Query.equal("status", "active")],
    10,
    0,
);

console.log(documents);
```

### Schema Definition

#### Static Schema (Repository Pattern)

```typescript
import { Entity, Column } from "@nuvix/database";

@Entity("users")
class User {
    @Column()
    id: number;

    @Column({ type: "string" })
    name: string;

    @Column({ type: "string" })
    email: string;
}
```

#### Dynamic Schema

```typescript
const col1 = db.createCollection(
    ID.uniqe(),
    [], // attributes
    [], // indexes
    true, // documentSecurity
    [Permission.read(Role.any())], // permissions
);

Document({
    $id: "6758383663783833983",
    attributes: [],
    indexes: [],
    documentSecurity: true,
    $permissions: [`read(any)`],
});
```

### Multi-Tenancy Support

Enable shared tables and tenant isolation:

```typescript
db.enableSharedTables(true);
db.setTenantId("tenant_1");

const tenantDocuments = await db.find("shared_table", []);
```

## MariaDB Adapter

The MariaDB adapter enables efficient interaction with MariaDB databases:

### Initialization

```typescript
const mariadbAdapter = new MariaDB({
    host: "localhost",
    user: "root",
    password: "password",
    database: "my_database",
});

const db = new Database(mariadbAdapter);
```

### Query Execution Example

```typescript
const documents = await db.find("products", [
    Query.contains("category", ["Electronics"]),
]);

console.log(documents);
```

## Logging and Debugging

Enable debug mode to log SQL queries and parameters:

```typescript
db.enableDebug(true);
```

## Contributing

We welcome contributions! Please follow these steps:

1. Fork the repository.
2. Create a feature branch: `git checkout -b feature-name`.
3. Commit your changes: `git commit -m 'Add some feature'`.
4. Push to the branch: `git push origin feature-name`.
5. Open a pull request.

## License

This project is licensed under the BSD 3-Clause License. See the [LICENSE](LICENSE) file for details.

## Acknowledgments

Special thanks to all the contributors and the open-source community for making this project possible.

---

Happy coding! 🎉
