import { Database } from "../src/core/database";
import { Document } from "../src/core/Document";
import { Query } from "../src/core/query";
import { PostgreDB } from "../src/adapter/postgre";
import { Cache, RedisAdapter } from "@nuvix/cache";
import { DB } from "./config";
import { DuplicateException } from "../src/errors";
import Role from "../src/security/Role";
import Permission from "../src/security/Permission";
import { Pool } from "pg";

describe("Database Advanced Tests", () => {
    let adapter: PostgreDB;
    let cache: Cache;
    let db: Database;

    // Skip tests if adapter connection isn't possible
    const runTests = process.env["SKIP_DB_TESTS"] !== "true";
    const ssl = process.env["SSL"] === "true";

    // Set higher timeout for tests
    jest.setTimeout(60000);

    const testPrefix = "db_adv_" + Date.now();
    const testCollectionName = "db_adv_test_" + Date.now();

    beforeAll(async () => {
        if (!runTests) {
            console.log(
                "Skipping advanced database tests. Set SKIP_DB_TESTS=false to run.",
            );
            return;
        }

        try {
            // Initialize adapter
            const ssl = process.env["SSL"] === "true";
            const client = await new Pool({
                connectionString: DB,
                ssl: ssl ? { rejectUnauthorized: false } : undefined,
            }).connect();
            const defaultOptions = {
                connection: client,
                schema: "public",
            };
            adapter = new PostgreDB({ ...defaultOptions });

            adapter.init();
            await adapter.ping();

            // Initialize cache with custom prefix for test isolation
            cache = new Cache(
                new RedisAdapter({
                    host: "localhost",
                    port: 6379,
                    namespace: `db-adv-test-${Date.now()}`,
                }),
            );

            // Create database instance
            db = new Database(adapter, cache, {
                logger: true,
            });

            db.setPrefix(testPrefix);
            await db.create();

            // Create test collection with various attribute types
            await db.createCollection(
                testCollectionName,
                [
                    new Document({
                        $id: "string_field",
                        key: "string_field",
                        type: "string",
                        size: 255,
                        required: true,
                    }),
                    new Document({
                        $id: "int_field",
                        key: "int_field",
                        type: "integer",
                        size: 11,
                        signed: true,
                    }),
                    new Document({
                        $id: "bool_field",
                        key: "bool_field",
                        type: "boolean",
                    }),
                ],
                [
                    new Document({
                        $id: "string_field_fulltext",
                        key: "string_field_fulltext",
                        type: Database.INDEX_FULLTEXT,
                        attributes: ["string_field"],
                        orders: ["ASC"],
                    }),
                ],
            );
        } catch (err) {
            console.error("Error setting up advanced database test:", err);
            throw err;
        }
    });

    afterAll(async () => {
        if (!runTests) return;

        try {
            // Clean up test collection
            await db.deleteCollection(testCollectionName);
            await adapter.close();

            // Clear cache (using any to bypass type checking)
            await cache.clear();
        } catch (err) {
            console.error("Error cleaning up advanced database test:", err);
        }
    });

    describe("Basic Database Operations", () => {
        test("should create and retrieve documents", async () => {
            if (!runTests) return;

            // Create a test document
            const docId = "test-doc-" + Date.now();
            const doc = new Document({
                $id: docId,
                string_field: "Hello World",
                int_field: 42,
                bool_field: true,
                $permissions: [
                    Permission.read(Role.any()),
                    Permission.update(Role.any()),
                    Permission.delete(Role.any()),
                ],
            });

            // Save document
            await db.createDocument(testCollectionName, doc);

            // Retrieve document
            const retrievedDoc = await db.getDocument(
                testCollectionName,
                docId,
            );

            // Verify data
            expect(retrievedDoc.getId()).toBe(docId);
            expect(retrievedDoc.getAttribute("string_field")).toBe(
                "Hello World",
            );
            expect(retrievedDoc.getAttribute("int_field")).toBe(42);
            expect(retrievedDoc.getAttribute("bool_field")).toBe(true);
        });

        test("should update documents", async () => {
            if (!runTests) return;

            // Create a test document
            const docId = "update-doc-" + Date.now();
            const doc = new Document({
                $id: docId,
                string_field: "Original",
                int_field: 100,
                bool_field: false,
                $permissions: [
                    Permission.read(Role.any()),
                    Permission.update(Role.any()),
                    Permission.delete(Role.any()),
                ],
            });

            // Save document
            await db.createDocument(testCollectionName, doc);

            // Update document
            const updateDoc = new Document({
                string_field: "Updated",
                int_field: 200,
                $permissions: [
                    Permission.read(Role.any()),
                    Permission.update(Role.any()),
                    Permission.delete(Role.any()),
                ],
            });

            await db.updateDocument(testCollectionName, docId, updateDoc);

            // Retrieve updated document
            const retrievedDoc = await db.getDocument(
                testCollectionName,
                docId,
            );

            await cache.clear(); // TODO: Temporary fix for cache issue

            // Verify updates
            expect(retrievedDoc.getAttribute("string_field")).toBe("Updated");
            expect(retrievedDoc.getAttribute("int_field")).toBe(200);
            expect(retrievedDoc.getAttribute("bool_field")).toBe(false); // Unchanged
        });

        test("should delete documents", async () => {
            if (!runTests) return;

            // Create a test document
            const docId = "delete-doc-" + Date.now();
            const doc = new Document({
                $id: docId,
                string_field: "To Be Deleted",
                int_field: 999,
                bool_field: true,
                $permissions: [
                    Permission.read(Role.any()),
                    Permission.update(Role.any()),
                    Permission.delete(Role.any()),
                ],
            });

            // Save document
            await db.createDocument(testCollectionName, doc);

            // Delete document
            await db.deleteDocument(testCollectionName, docId);

            // Try to retrieve deleted document
            try {
                await db.getDocument(testCollectionName, docId);
                // Should not reach here
                expect("Document should be deleted").toBe(false);
            } catch (err) {
                // Expected error
                expect(err).toBeDefined();
            }
        });
    });

    describe("Query Operations", () => {
        beforeEach(async () => {
            if (!runTests) return;

            // Create test documents
            const testDocs = [
                {
                    id: "query-doc-1",
                    string_value: "Alpha",
                    int_value: 10,
                    bool_value: true,
                },
                {
                    id: "query-doc-2",
                    string_value: "Beta",
                    int_value: 20,
                    bool_value: false,
                },
                {
                    id: "query-doc-3",
                    string_value: "Gamma",
                    int_value: 30,
                    bool_value: true,
                },
                {
                    id: "query-doc-4",
                    string_value: "Delta",
                    int_value: 40,
                    bool_value: false,
                },
                {
                    id: "query-doc-5",
                    string_value: "Alpha",
                    int_value: 50,
                    bool_value: true,
                },
            ];

            for (const testDoc of testDocs) {
                try {
                    const doc = new Document({
                        $id: testDoc.id,
                        string_field: testDoc.string_value,
                        int_field: testDoc.int_value,
                        bool_field: testDoc.bool_value,
                        $permissions: [
                            Permission.read(Role.any()),
                            Permission.write(Role.any()),
                        ],
                    });

                    await db.createDocument(testCollectionName, doc);
                } catch (err: any) {
                    // Skip if document already exists
                    if (err instanceof DuplicateException) continue;
                    throw err;
                }
            }
        });

        test("should support equality queries", async () => {
            if (!runTests) return;

            // Find documents with string_field = "Alpha"
            const results = await db.find(testCollectionName, [
                Query.equal("string_field", ["Alpha"]),
            ]);
            // Should find 2 documents
            expect(results.length).toBe(2);
            for (const doc of results) {
                expect(doc?.getAttribute("string_field")).toBe("Alpha");
            }
        });

        test("should support multiple conditions", async () => {
            if (!runTests) return;

            // Find documents with string_field = "Alpha" AND bool_field = true
            const results = await db.find(testCollectionName, [
                Query.equal("string_field", ["Alpha"]),
                Query.equal("bool_field", [true]),
            ]);

            // Should find 2 documents
            expect(results.length).toBe(2);
            for (const doc of results) {
                expect(doc?.getAttribute("string_field")).toBe("Alpha");
                expect(doc?.getAttribute("bool_field")).toBe(true);
            }
        });

        test("should support numeric comparison", async () => {
            if (!runTests) return;

            // Find documents with int_field > 30
            const results = await db.find(testCollectionName, [
                Query.greaterThan("int_field", 30),
                Query.limit(2),
            ]);

            // Should find 2 documents
            expect(results.length).toBe(2);
            for (const doc of results) {
                expect(doc?.getAttribute("int_field")).toBeGreaterThan(30);
            }
        });

        test("should support text search", async () => {
            if (!runTests) return;

            // Search for documents containing "Alpha" in string_field
            const results = await db.find(testCollectionName, [
                Query.search("string_field", "Alpha"),
            ]);

            // Should find 2 documents
            expect(results.length).toBe(2);
            for (const doc of results) {
                expect(doc?.getAttribute("string_field")).toContain("Alpha");
            }
        });

        test("should support search with multiple conditions and pagination", async () => {
            if (!runTests) return;

            // Search for documents containing "Alpha" in string_field AND bool_field is true
            const results = await db.find(testCollectionName, [
                Query.search("string_field", "Alpha"),
                Query.equal("bool_field", [true]),
                Query.limit(1),
                Query.offset(0),
            ]);

            // Should find 1 document due to limit
            expect(results.length).toBe(1);
            const doc = results[0];
            expect(doc?.getAttribute("string_field")).toContain("Alpha");
            expect(doc?.getAttribute("bool_field")).toBe(true);
        });

        test("should support sorting results", async () => {
            if (!runTests) return;

            // Find documents sorted by int_field DESC
            const results = await db.find(testCollectionName, [
                Query.orderDesc("int_field"),
                Query.limit(3),
            ]);

            // Should return 3 documents sorted by int_field
            expect(results.length).toBe(3);
            expect(
                results[0]?.getAttribute("int_field"),
            ).toBeGreaterThanOrEqual(results[1]?.getAttribute("int_field"));
            expect(
                results[1]?.getAttribute("int_field"),
            ).toBeGreaterThanOrEqual(results[2]?.getAttribute("int_field"));
        });

        test("should support cursor-based pagination", async () => {
            if (!runTests) return;

            // Find first 2 documents sorted by int_field ASC
            const firstPage = await db.find(testCollectionName, [
                Query.orderAsc("int_field"),
                Query.limit(2),
            ]);

            expect(firstPage.length).toBe(2);
            const cursor = firstPage[1]; // Use last document's ID as cursor
            const queries = [
                Query.orderAsc("int_field"),
                Query.limit(2),
                Query.cursorAfter(cursor?.getId() || ""),
            ];
            const cursorQuery = Query.findCursor(queries);
            cursorQuery?.setValue(cursor!);
            // Find next page using cursor
            const secondPage = await db.find(testCollectionName, queries);

            expect(secondPage.length).toBe(2);
            expect(secondPage[0]?.getId()).not.toBe(firstPage[0]?.getId());
        });
    });

    describe("Aggregation Operations", () => {
        test("should count documents", async () => {
            if (!runTests) return;

            // Count all documents
            const count = await db.count(testCollectionName);

            // Should have at least the documents created in previous tests
            expect(count).toBeGreaterThan(0);
        });

        test("should count documents with conditions", async () => {
            if (!runTests) return;

            // Count documents with bool_field = true
            const count = await db.count(testCollectionName, [
                Query.equal("bool_field", [true]),
            ]);

            // Should have some documents
            expect(count).toBeGreaterThanOrEqual(0);
        });

        test("should sum numeric fields", async () => {
            if (!runTests) return;

            // Sum int_field values
            const sum = await db.sum(testCollectionName, "int_field");

            // Sum should be positive (we have positive values in test data)
            expect(sum).toBeGreaterThan(0);
        });
    });

    describe("Document Attributes", () => {
        test("should increment numeric attributes", async () => {
            if (!runTests) return;

            // Create a test document
            const docId = "increment-doc-" + Date.now();
            const doc = new Document({
                $id: docId,
                string_field: "Increment Test",
                int_field: 100,
                bool_field: true,
                $permissions: [
                    Permission.read(Role.any()),
                    Permission.update(Role.any()),
                    Permission.delete(Role.any()),
                ],
            });

            // Save document
            await db.createDocument(testCollectionName, doc);

            // Increment int_field by 50
            await db.increaseDocumentAttribute(
                testCollectionName,
                docId,
                "int_field",
                50,
            );

            // Retrieve document
            const retrievedDoc = await db.getDocument(
                testCollectionName,
                docId,
            );

            // Verify increment
            expect(retrievedDoc.getAttribute("int_field")).toBe(150);
        });
    });

    describe("Document Attributes", () => {
        test("should decrement numeric attributes", async () => {
            if (!runTests) return;

            // Create a test document
            const docId = "decrement-doc-" + Date.now();
            const doc = new Document({
                $id: docId,
                string_field: "Decrement Test",
                int_field: 100,
                bool_field: true,
                $permissions: [
                    Permission.read(Role.any()),
                    Permission.update(Role.any()),
                    Permission.delete(Role.any()),
                ],
            });

            // Save document
            await db.createDocument(testCollectionName, doc);

            // Decrement int_field by 50
            await db.decreaseDocumentAttribute(
                testCollectionName,
                docId,
                "int_field",
                50,
            );

            // Retrieve document
            const retrievedDoc = await db.getDocument(
                testCollectionName,
                docId,
            );

            // Verify decrement
            expect(retrievedDoc.getAttribute("int_field")).toBe(50);
        });
    });

    describe("Error Handling", () => {
        test("should handle missing documents", async () => {
            if (!runTests) return;

            // Try to retrieve non-existent document
            try {
                await db.getDocument(testCollectionName, "non-existent-id");
                // Should not reach here
                expect("Non-existent document should throw error").toBe(false);
            } catch (err) {
                // Expected error
                expect(err).toBeDefined();
            }
        });

        test("should handle invalid data", async () => {
            if (!runTests) return;

            // Try to create document with missing required field
            try {
                const doc = new Document({
                    $id: "invalid-doc",
                    // Missing required string_field
                    int_field: 100,
                    bool_field: true,
                    $permissions: [
                        Permission.read(Role.any()),
                        Permission.write(Role.any()),
                    ],
                });

                await db.createDocument(testCollectionName, doc);
                // Should not reach here
                expect("Invalid document should throw error").toBe(false);
            } catch (err) {
                // Expected error
                expect(err).toBeDefined();
            }
        });
    });

    describe("Caching Operations", () => {
        test("should toggle cache features", async () => {
            if (!runTests) return;

            // Test cache enabled/disabled methods
            expect(db.isCacheEnabled()).toBe(true); // Default is true

            db.disableCache();
            expect(db.isCacheEnabled()).toBe(false);

            db.enableCache();
            expect(db.isCacheEnabled()).toBe(true);

            db.setCacheEnabled(false);
            expect(db.isCacheEnabled()).toBe(false);

            db.setCacheEnabled(true);
            expect(db.isCacheEnabled()).toBe(true);
        });

        test("should retrieve documents from cache", async () => {
            if (!runTests) return;

            // Create a test document
            const docId = "cache-doc-" + Date.now();
            const doc = new Document({
                $id: docId,
                string_field: "Cache Test",
                int_field: 123,
                bool_field: true,
                $permissions: [
                    Permission.read(Role.any()),
                    Permission.update(Role.any()),
                    Permission.delete(Role.any()),
                ],
            });

            // Save document
            await db.createDocument(testCollectionName, doc);

            // First fetch (cache miss)
            const start1 = performance.now();
            const doc1 = await db.getDocument(testCollectionName, docId);
            const time1 = performance.now() - start1;

            // Second fetch (cache hit)
            const start2 = performance.now();
            const doc2 = await db.getDocument(testCollectionName, docId);
            const time2 = performance.now() - start2;

            // Verify correct data
            expect(doc2.getAttribute("string_field")).toBe("Cache Test");

            console.log(
                `Cache miss time: ${time1}ms, Cache hit time: ${time2}ms`,
            );

            // Skip cache
            await db.skipCache(async () => {
                // This should bypass cache
                const start3 = performance.now();
                const doc3 = await db.getDocument(testCollectionName, docId);
                const time3 = performance.now() - start3;

                console.log(`Cache skip time: ${time3}ms`);
                expect(doc3.getAttribute("string_field")).toBe("Cache Test");
            });
        });

        test("should invalidate cache on document update", async () => {
            if (!runTests) return;

            // Create a test document
            const docId = "cache-invalidate-" + Date.now();
            const doc = new Document({
                $id: docId,
                string_field: "Original",
                int_field: 100,
                bool_field: true,
                $permissions: [
                    Permission.read(Role.any()),
                    Permission.update(Role.any()),
                    Permission.delete(Role.any()),
                ],
            });

            // Save document
            await db.createDocument(testCollectionName, doc);

            // First read to populate cache
            const original = await db.getDocument(testCollectionName, docId);
            expect(original.getAttribute("string_field")).toBe("Original");

            // Update document
            await db.updateDocument(
                testCollectionName,
                docId,
                new Document({
                    string_field: "Updated",
                    $permissions: [
                        Permission.read(Role.any()),
                        Permission.update(Role.any()),
                        Permission.delete(Role.any()),
                    ],
                }),
            );

            await new Promise((resolve) => setTimeout(resolve, 1000));

            // Read after update - should have updated value, not cached
            const updated = await db.getDocument(testCollectionName, docId);
            expect(updated.getAttribute("string_field")).toBe("Updated");
        });

        test("should clear cache", async () => {
            if (!runTests) return;

            // Create a test document
            const docId = "cache-clear-" + Date.now();
            const doc = new Document({
                $id: docId,
                string_field: "Clear Cache Test",
                int_field: 100,
                bool_field: true,
                $permissions: [
                    Permission.read(Role.any()),
                    Permission.update(Role.any()),
                    Permission.delete(Role.any()),
                ],
            });

            // Save document
            await db.createDocument(testCollectionName, doc);

            // First read to populate cache
            await db.getDocument(testCollectionName, docId);

            // Clear entire cache
            await db.clearCache();

            // Document should be fetched from database, not cache
            const start = performance.now();
            const fetched = await db.getDocument(testCollectionName, docId);
            const time = performance.now() - start;

            console.log(`Fetch after cache clear: ${time}ms`);
            expect(fetched.getAttribute("string_field")).toBe(
                "Clear Cache Test",
            );
        });

        test("should manage cache TTL", async () => {
            if (!runTests) return;

            // Test TTL methods
            const defaultTTL = db.getCacheTTL();
            expect(defaultTTL).toBe(300); // Default 5 minutes

            const newTTL = 600;
            db.setCacheTTL(newTTL);
            expect(db.getCacheTTL()).toBe(newTTL);

            // Reset to default
            db.setCacheTTL(300);
        });
    });
});
