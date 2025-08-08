import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { Database } from '@core/database.js';
import { createTestDb } from './helpers.js';
import { Doc } from '@core/doc.js';
import { AttributeEnum, IndexEnum } from '@core/enums.js';
import { Permission } from '@utils/permission.js';
import { Role } from '@utils/role.js';

let db: Database;

describe('Database - basic collections and documents', () => {
    beforeAll(async () => {
        db = createTestDb({ namespace: `db_basic_${Date.now()}` });
        await db.create('public');
    });

    afterAll(async () => {
        await db.getAdapter().$client.disconnect();
    });

    it('creates metadata and a user collection, then inserts and reads a document', async () => {
        // create a collection
        const attributes = [
            new Doc({ $id: 'name', key: 'name', type: AttributeEnum.String, size: 128, required: true }),
            new Doc({ $id: 'age', key: 'age', type: AttributeEnum.Integer, size: 4 }),
        ];

        const indexes = [
            new Doc({ $id: '_index_name', key: '_index_name', type: IndexEnum.Key, attributes: ['name'] }),
        ];

        const collection = await db.createCollection({
            id: 'users',
            attributes,
            indexes,
            permissions: [Permission.create(Role.any())],
            documentSecurity: false,
        });

        expect(collection.get('$id')).toBe('users');

        // create doc
        const created = await db.createDocument('users', new Doc({
            name: 'Ada',
            age: 30,
            $permissions: [Permission.read(Role.any()).toString()],
        }));

        expect(created.get('$id')).toBeTruthy();
        expect(created.get('name')).toBe('Ada');

        // get doc
        const got = await db.getDocument('users', created.get('$id'));
        expect(got.get('name')).toBe('Ada');
    });
});
