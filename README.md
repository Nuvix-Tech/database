# Document Class

The `Document` class is a flexible and strongly-typed utility designed for handling structured data with advanced features like attribute manipulation, permission management, and serialization. This README provides an overview of its usage and capabilities.

---

## Features

1. **Strong Typing**: TypeScript generics ensure type safety for document attributes.
2. **Immutable Patterns**: Supports immutable updates to avoid unintended mutations.
3. **Advanced Permissions Handling**: Easy-to-use methods for managing read, write, create, update, and delete permissions.
4. **Serialization Options**: Flexible export options for JSON and custom formats.
5. **Hooks for Validation**: Lifecycle hooks like `beforeSave` for extensibility.
6. **Deep Cloning**: Built-in support for creating deep copies of documents.

---

## Installation

To use the `Document` class in your project:

```bash
npm install @your-namespace/document
```

---

## Usage

### Import the Class

```typescript
import { Document } from '@your-namespace/document';
```

### Basic Usage

Create a new document with attributes:

```typescript
const doc = new Document<{ id: string; name: string; permissions: string[] }>(
    { id: '123', name: 'My Document', permissions: ['read(user1)', 'write(user2)'] }
);

console.log(doc.getAttribute('name')); // "My Document"
```

### Updating Attributes

Immutable updates:

```typescript
const updatedDoc = doc.cloneWith({ name: 'Updated Document' });
console.log(updatedDoc.getAttribute('name')); // "Updated Document"
```

Mutable updates:

```typescript
doc.setAttribute('name', 'Updated Name');
console.log(doc.getAttribute('name')); // "Updated Name"
```

### Permissions Management

Extract permissions by type:

```typescript
const readPermissions = doc.getPermissionsByType('read');
console.log(readPermissions); // ["user1"]
```

### Serialization

Export document attributes to JSON:

```typescript
const json = doc.toJSON();
console.log(json); // { id: '123', name: 'My Document', permissions: [...] }
```

Export with filters:

```typescript
const filteredJson = doc.toJSON(['id']);
console.log(filteredJson); // { id: '123' }
```

### Lifecycle Hooks

Extend the class to implement custom hooks:

```typescript
class CustomDocument extends Document {
    async beforeSave(): Promise<void> {
        console.log('Validating document...');
    }

    async save(): Promise<this> {
        await this.beforeSave();
        // Save logic here
        return this;
    }
}
```

---

## API Reference

### Constructor

```typescript
new Document<T extends Record<string, any>>(attributes: T);
```

- **attributes**: Initial attributes for the document.

### Methods

#### `getAttribute`

```typescript
getAttribute<K extends keyof T>(key: K, defaultValue?: T[K]): T[K] | undefined;
```

Retrieve an attribute value by key.

#### `setAttribute`

```typescript
setAttribute<K extends keyof T>(key: K, value: T[K]): this;
```

Set or update an attribute.

#### `cloneWith`

```typescript
cloneWith(changes: Partial<T>): Document<T>;
```

Create a new document with updated attributes.

#### `getPermissionsByType`

```typescript
getPermissionsByType(type: string): string[];
```

Retrieve permissions filtered by type (e.g., `read`, `write`).

#### `toJSON`

```typescript
toJSON(allowedKeys?: (keyof T)[]): Partial<T>;
```

Export document attributes as a plain object.

---

## Testing

Run the test suite to ensure all features work as expected:

```bash
npm test
```

---

## License

This project is licensed under the BSD 3-Clause License. See the [LICENSE](LICENSE) file for details.

---

## Contributing

We welcome contributions! Please read our [CONTRIBUTING](CONTRIBUTING.md) guide to get started.

---

## Changelog

See the [CHANGELOG](CHANGELOG.md) for details about recent updates.

---

## Feedback

Feel free to open an issue or submit a pull request on our [GitHub repository](https://github.com/your-repo/document).

---

Happy coding! 🚀

