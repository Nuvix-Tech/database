import { describe, test, expect, beforeEach, afterEach } from 'vitest';
import { Database } from '@core/database.js';
import { createTestDb } from '../helpers.js';
import { DatabaseException } from '@errors/index.js';

describe('Database Operations', () => {
    let db: Database;
    const schema = new Date().getTime().toString()

    beforeEach(async () => {
        db = createTestDb({ namespace: `coll_op_${schema}` });
        db.setMeta({ schema })
        await db.create();
    });

    afterEach(async () => {
        await db.delete();
    });

    describe('create', () => {
        test('should create a new database', async () => {
            const dbName = `test_db_${Date.now()}`;
            await db.create(dbName);

            const exists = await db.exists(dbName);
            expect(exists).toBe(true);
        });

        test('should create database with default name if not provided', async () => {
            await db.create();

            const exists = await db.exists();
            expect(exists).toBe(true);
        });

        test('should create metadata collection during database creation', async () => {
            const dbName = `test_metadata_${Date.now()}`;
            await db.create(dbName);

            const collection = await db.getCollection(Database.METADATA);
            expect(collection.empty()).toBe(false);
            expect(collection.getId()).toBe(Database.METADATA);
        });
    });

    describe('exists', () => {
        test('should return true for existing database', async () => {
            const dbName = `test_exists_${Date.now()}`;
            await db.create(dbName);

            const exists = await db.exists(dbName);
            expect(exists).toBe(true);
        });

        test('should return false for non-existing database', async () => {
            const nonExistentDb = `non_existent_${Date.now()}`;
            const exists = await db.exists(nonExistentDb);
            expect(exists).toBe(false);
        });

        test('should check collection existence', async () => {
            await db.create();

            // Metadata collection should exist
            const metadataExists = await db.exists(undefined, Database.METADATA);
            expect(metadataExists).toBe(true);

            // Non-existent collection should not exist
            const nonExistentExists = await db.exists(undefined, 'non_existent_collection');
            expect(nonExistentExists).toBe(false);
        });
    });

    describe('list', () => {
        test('should return empty array by default', async () => {
            const databases = await db.list();
            expect(Array.isArray(databases)).toBe(true);
            expect(databases).toEqual([]);
        });
    });

    describe('delete', () => {
        test('should delete existing database', async () => {
            const dbName = `test_delete_${Date.now()}`;
            await db.create(dbName);

            // Verify it exists
            let exists = await db.exists(dbName);
            expect(exists).toBe(true);

            // Delete it
            await db.delete(dbName);

            // Verify it no longer exists
            exists = await db.exists(dbName);
            expect(exists).toBe(false);
        });

        test('should delete default database if no name provided', async () => {
            await db.create();
            await db.delete();

            // Should not throw error when deleting non-existent database
            await expect(db.delete()).resolves.not.toThrow();
        });

        test('should flush cache after deletion', async () => {
            const dbName = `test_cache_flush_${Date.now()}`;
            await db.create(dbName);

            // Add some data to cache through collection operations
            await db.createCollection({
                id: 'test_collection',
                attributes: []
            });

            await db.delete(dbName);

            // Cache should be flushed - attempting to get collection should fail
            const collection = await db.getCollection('test_collection');
            expect(collection.empty()).toBe(true);
        });
    });

    describe('error handling', () => {
        test('should handle database creation errors gracefully', async () => {
            // Test with invalid database name (if adapter supports validation)
            await expect(async () => {
                await db.create(''); // Empty name
            }).rejects.toThrow();
        });

        test('should handle deletion of non-existent database', async () => {
            const nonExistentDb = `non_existent_delete_${Date.now()}`;

            // Should not throw error
            await expect(db.delete(nonExistentDb)).resolves.not.toThrow();
        });
    });

    describe('edge cases', () => {
        test('should handle concurrent database operations', async () => {
            const dbName1 = `concurrent_db1_${Date.now()}`;
            const dbName2 = `concurrent_db2_${Date.now()}`;

            // Create databases concurrently
            await Promise.all([
                db.create(dbName1),
                db.create(dbName2)
            ]);

            // Both should exist
            const [exists1, exists2] = await Promise.all([
                db.exists(dbName1),
                db.exists(dbName2)
            ]);

            expect(exists1).toBe(true);
            expect(exists2).toBe(true);
        });

        test('should handle multiple database deletions', async () => {
            const dbNames = Array.from({ length: 3 }, (_, i) => `multi_delete_${i}_${Date.now()}`);

            // Create multiple databases
            await Promise.all(dbNames.map(name => db.create(name)));

            // Delete all
            await Promise.all(dbNames.map(name => db.delete(name)));

            // None should exist
            const existsResults = await Promise.all(dbNames.map(name => db.exists(name)));
            expect(existsResults.every(exists => !exists)).toBe(true);
        });
    });
});
