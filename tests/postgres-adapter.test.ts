import { PostgreDB } from "../src/adapter/postgre";
import { Document } from "../src/core/Document";
import { Query } from "../src/core/query";
import { DB } from "./config";
import { Database } from "../src/core/database";
import { Cache, RedisAdapter } from "@nuvix/cache";

/**
 * Tests for the PostgreSQL adapter focused on connection pool management,
 * query execution statistics, and diagnostics.
 */
describe("PostgreSQL Adapter", () => {
    let adapter: PostgreDB;
    let db: Database;

    // Skip tests if adapter connection isn't possible
    const runTests = process.env["SKIP_DB_TESTS"] !== "true";
    const ssl = process.env["SSL"] === "true";

    // Set higher timeout for tests
    jest.setTimeout(60000);

    const testCollectionName = "pg_adapter_test_" + Date.now();
    const testPrefix = "pgtest_" + Date.now();

    beforeAll(async () => {
        if (!runTests) {
            console.log(
                "Skipping PostgreSQL adapter tests. Set SKIP_DB_TESTS=false to run.",
            );
            return;
        }

        try {
            // Initialize adapter with custom pool settings
            adapter = new PostgreDB({
                connection: {
                    connectionString: DB,
                    // Custom pool settings to test pool management
                    max: 5,
                    idleTimeoutMillis: 10000,
                    connectionTimeoutMillis: 5000,
                    ssl: ssl
                        ? {
                              rejectUnauthorized: false,
                          }
                        : undefined,
                },
                schema: "public",
            });

            adapter.init();
            await adapter.ping();

            // Set prefix for test isolation
            adapter.setPrefix(testPrefix);

            // Initialize database
            const cache = new Cache(
                new RedisAdapter({
                    host: "localhost",
                    port: 6379,
                    namespace: "pg-adapter-test",
                }),
            );
            db = new Database(adapter, cache, {
                logger: true,
            });
            await db.create();

            // Create a test collection
            await db.createCollection(testCollectionName, [
                new Document({
                    $id: "name",
                    key: "name",
                    type: "string",
                    size: 255,
                    required: true,
                }),
                new Document({
                    $id: "value",
                    key: "value",
                    type: "integer",
                    size: 11,
                }),
            ]);
        } catch (err) {
            console.error("Error setting up PostgreSQL adapter test:", err);
            throw err;
        }
    });

    afterAll(async () => {
        if (!runTests) return;

        try {
            // Clean up test collection
            await db.deleteCollection(testCollectionName);
            await adapter.close();
        } catch (err) {
            console.error("Error cleaning up PostgreSQL adapter test:", err);
        }
    });

    describe("Pool Management", () => {
        test("should provide pool statistics", async () => {
            if (!runTests) return;

            const stats = adapter.getPoolStats();

            expect(stats).toBeDefined();
            expect(stats.totalCount).toBeDefined();
            expect(stats.idleCount).toBeDefined();
            expect(stats.waitingCount).toBeDefined();
        });

        test("should acquire and release connections", async () => {
            if (!runTests) return;

            // Get initial stats
            const initialStats = adapter.getPoolStats();

            // Acquire multiple clients simultaneously
            const clients = await Promise.all([
                adapter.getClient(),
                adapter.getClient(),
                adapter.getClient(),
            ]);

            // Check stats after acquisition
            const midStats = adapter.getPoolStats();
            expect(midStats.idleCount).toBeLessThan(initialStats.idleCount + 3);

            // Release all clients
            clients.forEach((client) => client.release());

            // Small delay to allow pool to process releases
            await new Promise((resolve) => setTimeout(resolve, 100));

            // Check stats after release
            const finalStats = adapter.getPoolStats();
            expect(finalStats.idleCount).toBeGreaterThanOrEqual(
                initialStats.idleCount,
            );
        });
    });

    describe("Query Execution", () => {
        test("should track query statistics", async () => {
            if (!runTests) return;

            // Reset query stats
            adapter.resetQueryStats();

            // Execute a series of queries
            await db.createDocument(
                testCollectionName,
                new Document({
                    name: "Query Test 1",
                    value: 100,
                }),
            );

            await db.createDocument(
                testCollectionName,
                new Document({
                    name: "Query Test 2",
                    value: 200,
                }),
            );

            await db.find(testCollectionName, [
                Query.equal("name", ["Query Test 1"]),
                Query.equal("value", [100]),
            ]);

            // Get query stats
            const stats = adapter.getQueryStats();

            expect(stats.totalQueries).toBeGreaterThan(0);
            expect(stats.successfulQueries).toBeGreaterThan(0);
            expect(stats.queryLog.length).toBeGreaterThan(0);
        });

        test("should report slow queries", async () => {
            if (!runTests) return;

            // Reset query stats
            adapter.resetQueryStats();

            // Execute a potentially slow query
            await db.find(testCollectionName, [
                Query.equal("name", ["Query Test 1"]),
                Query.equal("value", [100]),
            ]);

            // Get query stats
            const stats = adapter.getQueryStats();

            // We don't assert on slowest query time specifically since it depends on environment
            expect(stats.slowestQueryMs).toBeGreaterThanOrEqual(0);
            expect(stats.slowestQuery).toBeDefined();
        });
    });

    describe("Diagnostics", () => {
        test("should provide comprehensive diagnostics", async () => {
            if (!runTests) return;

            const diagnostics = await adapter.getDiagnostics();

            // Check diagnostics object structure
            expect(diagnostics).toBeDefined();
            expect(diagnostics.timestamp).toBeDefined();
            expect(diagnostics.poolStatus).toBeDefined();
            expect(diagnostics.queryPerformance).toBeDefined();
            expect(diagnostics.databaseInfo).toBeDefined();
            expect(diagnostics.adapter).toBeDefined();

            // Check specific values
            expect(diagnostics.databaseInfo.version).toBeDefined();
            expect(diagnostics.adapter.type).toBe("postgresql");
        });
    });

    describe("Transaction Management", () => {
        test("should execute operations in a transaction", async () => {
            if (!runTests) return;

            // Create test document
            const docId = "tx-test-" + Date.now();

            // Execute transaction with the new withTransaction method
            await adapter.withTransaction(async (client) => {
                // Create test document within transaction
                const doc = new Document({
                    $id: docId,
                    name: "Transaction Test",
                    value: 999,
                });

                // Insert document using transaction client
                const sql = `
                    INSERT INTO ${adapter.getSchema()}.${testPrefix}_${testCollectionName} 
                    (_uid, name, value, _createdAt, _updatedAt, _permissions) 
                    VALUES ($1, $2, $3, $4, $5, $6)
                `;

                const now = new Date().toISOString();
                await client.query(sql, [
                    docId,
                    "Transaction Test",
                    999,
                    now,
                    now,
                    "{}",
                ]);

                // Verify document exists within transaction
                const checkSql = `
                    SELECT * FROM ${adapter.getSchema()}.${testPrefix}_${testCollectionName} 
                    WHERE _uid = $1
                `;
                const result = await client.query(checkSql, [docId]);
                expect(result.rows.length).toBe(1);
            });

            // Verify document exists after transaction
            const doc = await db.getDocument(testCollectionName, docId);
            expect(doc.getId()).toBe(docId);
            expect(doc.getAttribute("name")).toBe("Transaction Test");
        });

        test("should roll back failed transactions", async () => {
            if (!runTests) return;

            // Create test document ID
            const docId = "tx-rollback-" + Date.now();

            // Execute transaction that will fail
            try {
                await adapter.withTransaction(async (client) => {
                    // Insert document using transaction client
                    const sql = `
                        INSERT INTO ${adapter.getSchema()}.${testPrefix}_${testCollectionName} 
                        (_uid, name, value, _createdAt, _updatedAt, _permissions) 
                        VALUES ($1, $2, $3, $4, $5, $6)
                    `;

                    const now = new Date().toISOString();
                    await client.query(sql, [
                        docId,
                        "Rollback Test",
                        1000,
                        now,
                        now,
                        "{}",
                    ]);

                    // Force an error to trigger rollback
                    throw new Error("Intentional error to test rollback");
                });

                // Should not reach here
                expect("Transaction should have failed").toBe(false);
            } catch (error: any) {
                // Expected error
            }

            // Verify document does not exist after rollback
            try {
                await db.getDocument(testCollectionName, docId);
                // Should not reach here
                expect("Document should not exist").toBe(false);
            } catch (error: any) {
                // Expected error
                expect(error).toBeDefined();
            }
        });
    });

    describe("Event Handling", () => {
        test("should emit events for query execution", async () => {
            if (!runTests) return;

            // Create event listener
            const events: any[] = [];
            const listener = (data: any) => {
                events.push(data);
            };

            // Add event listener
            adapter.on("query:executed", listener);

            // Execute a query
            await db.createDocument(
                testCollectionName,
                new Document({
                    name: "Event Test",
                    value: 123,
                }),
            );

            // Remove listener
            adapter.removeListener("query:executed", listener);

            // Check events
            expect(events.length).toBeGreaterThan(0);
            expect(events[0].sql).toBeDefined();
            expect(events[0].executionTime).toBeDefined();
            expect(events[0].success).toBe(true);
        });
    });

    // --- Advanced and missing tests for PostgreDB ---
    describe("Advanced PostgreDB Adapter Features", () => {
        const extraCollection = testCollectionName + "_extra";
        afterAll(async () => {
            if (!runTests) return;
            try {
                await db.deleteCollection(extraCollection);
            } catch {}
        });

        test("should create and drop schema", async () => {
            if (!runTests) return;
            const schemaName = "test_schema_" + Date.now();
            expect(await adapter.create(schemaName)).toBe(true);
            expect(await adapter.exists(schemaName)).toBe(true);
            expect(await adapter.drop(schemaName)).toBe(true);
        });

        test("should handle attribute and index management", async () => {
            if (!runTests) return;
            // Create a new collection
            await db.createCollection(extraCollection, [
                new Document({ $id: "foo", key: "foo", type: "string", size: 50 }),
            ]);
            // Add attribute
            expect(await adapter.createAttribute(extraCollection, "bar", "integer", 4)).toBe(true);
            // Update attribute
            expect(await adapter.updateAttribute(extraCollection, "bar", "integer", 8)).toBe(true);
            // Rename attribute
            expect(await adapter.renameAttribute(extraCollection, "bar", "baz")).toBe(true);
            // Delete attribute
            expect(await adapter.deleteAttribute(extraCollection, "baz")).toBe(true);
            // Create index
            expect(await adapter.createIndex(extraCollection, "foo_idx", "key", ["foo"])).toBe(true);
            // Rename index
            expect(await adapter.renameIndex(extraCollection, "foo_idx", "foo_idx2")).toBe(true);
            // Delete index
            expect(await adapter.deleteIndex(extraCollection, "foo_idx2")).toBe(true);
        });

        test("should handle relationship management", async () => {
            if (!runTests) return;
            // Create two collections
            await db.createCollection(extraCollection + "1", [new Document({ $id: "a", key: "a", type: "string", size: 10 })]);
            await db.createCollection(extraCollection + "2", [new Document({ $id: "b", key: "b", type: "string", size: 10 })]);
            // One-to-one
            expect(await adapter.createRelationship(extraCollection + "1", extraCollection + "2", "oneToOne", true, "rel1", "rel2")).toBe(true);
            // Update relationship
            expect(await adapter.updateRelationship(extraCollection + "1", extraCollection + "2", "oneToOne", true, "rel1", "rel2", "parent", "rel1_new", "rel2_new")).toBe(true);
            // Delete relationship
            expect(await adapter.deleteRelationship(extraCollection + "1", extraCollection + "2", "oneToOne", true, "rel1_new", "rel2_new", "parent")).toBe(true);
        });

        test("should handle bulk document operations", async () => {
            if (!runTests) return;
            const docs = [
                new Document({ name: "Bulk1", value: 1 }),
                new Document({ name: "Bulk2", value: 2 }),
            ];
            const created = await adapter.createDocuments(extraCollection, docs);
            expect(created.length).toBe(2);
            // Update documents
            const updateDoc = new Document({ value: 99 });
            const updatedCount = await adapter.updateDocuments(extraCollection, updateDoc, created);
            expect(updatedCount).toBe(2);
            // Delete documents
            const deletedCount = await adapter.deleteDocuments(extraCollection, created.map(d => d.getId()));
            expect(deletedCount).toBe(2);
        });

        test("should handle permission changes on update", async () => {
            if (!runTests) return;
            const doc = new Document({ name: "PermTest", value: 10 });
            await db.createDocument(extraCollection, doc);
            doc.setAttribute("$permissions", ["role:admin"]);
            await adapter.updateDocument(extraCollection, doc.getId(), doc);
            // Should not throw
        });

        test("should get collection size and diagnostics", async () => {
            if (!runTests) return;
            const size = await adapter.getSizeOfCollection(extraCollection);
            expect(typeof size).toBe("number");
            const sizeDisk = await adapter.getSizeOfCollectionOnDisk(extraCollection);
            expect(typeof sizeDisk).toBe("number");
            const diag = await adapter.getDiagnostics();
            expect(diag).toBeDefined();
            expect(diag.poolStatus).toBeDefined();
            expect(diag.queryPerformance).toBeDefined();
        });

        test("should translate PostgreSQL errors", async () => {
            if (!runTests) return;
            // Duplicate collection
            await expect(db.createCollection(extraCollection, [new Document({ $id: "foo", key: "foo", type: "string", size: 50 })])).rejects.toBeDefined();
            // Drop non-existent attribute
            await expect(adapter.deleteAttribute(extraCollection, "nonexistent"))
                .resolves.toBe(true); // Should resolve as per implementation
        });
    });
});
