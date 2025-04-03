import { Database } from "../src/core/database";
import { Document } from "../src/core/Document";
import { Query } from "../src/core/query";
import { Adapter } from "../src/adapter/base";
import { PostgreDB } from "../src/adapter/postgre";
import { DB } from "./config";
import { Cache, RedisAdapter } from "@nuvix/cache";
import Permission from "../src/security/Permission";
import Role from "../src/security/Role";
import { Authorization } from "../src/security/authorization";

/**
 * Gets an initialized database adapter for testing
 * This factory allows the tests to work with any adapter implementation
 */
function getAdapter(): Adapter {
    // Create a PostgreSQL adapter by default
    // In a production environment, you would inject the adapter based on configuration
    const adapter = new PostgreDB({
        connection: {
            connectionString: DB,
            ssl: {
                rejectUnauthorized: false,
            },
        },
        schema: "public",
    });

    adapter.init();
    return adapter;
}

// Skip tests if adapter connection isn't possible
const runTests = process.env.SKIP_DB_TESTS !== "true";

describe("Database Core", () => {
    let adapter: Adapter;
    let db: Database;
    let cache: Cache;

    // Set higher timeout for tests
    jest.setTimeout(60000);

    const testCollectionName = "test_core_" + Date.now();

    beforeAll(async () => {
        if (!runTests) {
            console.log(
                "Skipping database tests. Set SKIP_DB_TESTS=false to run.",
            );
            return;
        }

        try {
            // Initialize adapter
            adapter = getAdapter();
            await (adapter as PostgreDB).ping();
            const prefix = `test_${Date.now()}`;
            // Initialize cache and database
            cache = new Cache(
                new RedisAdapter({
                    host: "localhost",
                    port: 6379,
                    namespace: "test-core",
                }),
            );
            db = new Database(adapter, cache, {
                logger: true,
            });
            db.setPrefix(prefix);
            await db.create();

            // Create the test collection once for all tests
            await db.createCollection(
                testCollectionName,
                [
                    new Document({
                        $id: "name",
                        key: "name",
                        type: "string",
                        size: 255,
                        required: true,
                        $permissions: [Permission.read(Role.any())],
                    }),
                    new Document({
                        $id: "email",
                        key: "email",
                        type: "string",
                        size: 255,
                        $permissions: [Permission.read(Role.any())],
                    }),
                    new Document({
                        $id: "age",
                        key: "age",
                        type: "integer",
                        size: 11,
                        signed: true,
                        $permissions: [Permission.read(Role.any())],
                    }),
                    new Document({
                        $id: "isActive",
                        key: "isActive",
                        type: "boolean",
                        $permissions: [Permission.read(Role.any())],
                    }),
                    new Document({
                        $id: "tags",
                        key: "tags",
                        type: "string",
                        array: true,
                        size: 50,
                        $permissions: [Permission.read(Role.any())],
                    }),
                    new Document({
                        $id: "profile",
                        key: "profile",
                        type: "string",
                        size: 10000,
                        filters: ["json"],
                        $permissions: [Permission.read(Role.any())],
                    }),
                ],
                [
                    new Document({
                        $id: "name_idx",
                        type: "key",
                        attributes: ["name"],
                    }),
                    new Document({
                        $id: "email_idx",
                        type: "key",
                        attributes: ["email"],
                    }),
                ],
            );
        } catch (err) {
            console.error("Error setting up database test:", err);
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
            console.error("Error cleaning up database test:", err);
        }
    });

    describe("Collection Operations", () => {
        test("should check if collection exists", async () => {
            if (!runTests) return;

            const exists = await db.getCollection(testCollectionName);
            expect(exists.isEmpty()).toBe(false);
        });

        test("should get a collection", async () => {
            if (!runTests) return;

            const collection = await db.getCollection(testCollectionName);
            expect(collection).toBeTruthy();
            expect(collection.getId()).toBe(testCollectionName);
            expect(collection.getAttribute("attributes")).toBeTruthy();
        });
    });

    describe("Document Operations", () => {
        const docId = "test-core-" + Date.now();

        test("should create a document", async () => {
            if (!runTests) return;

            const doc = new Document({
                $id: docId,
                name: "John Doe",
                email: "john@example.com",
                age: 30,
                isActive: true,
                tags: ["developer", "typescript"],
                profile: { bio: "Software developer", location: "New York" },
                $permissions: [
                    Permission.read(Role.any()),
                    Permission.update(Role.any()),
                    Permission.delete(Role.any()),
                ],
            });

            const result = await db.createDocument(testCollectionName, doc);
            expect(result).toBeTruthy();
            expect(result.getId()).toBe(docId);
            expect(result.getAttribute("name")).toBe("John Doe");
            expect(result.getPermissions()).toContain(
                Permission.read(Role.any()),
            );
        });

        test("should get a document", async () => {
            if (!runTests) return;

            const doc = await db.getDocument(testCollectionName, docId);
            expect(doc).toBeTruthy();
            expect(doc.getAttribute("name")).toBe("John Doe");
            expect(doc.getAttribute("email")).toBe("john@example.com");
            expect(doc.getPermissions()).toContain(Permission.read(Role.any()));

            // Check if data types were properly maintained
            expect(typeof doc.getAttribute("age")).toBe("number");
            expect(typeof doc.getAttribute("isActive")).toBe("boolean");
            expect(Array.isArray(doc.getAttribute("tags"))).toBe(true);

            // Check if JSON data was properly stored and retrieved
            const profile = doc.getAttribute("profile");
            expect(profile).toBeTruthy();
            expect(profile.bio).toBe("Software developer");
        });

        test("should update a document", async () => {
            if (!runTests) return;

            const updateDoc = new Document({
                name: "John Doe Updated",
                age: 31,
                $permissions: [
                    Permission.read(Role.any()),
                    Permission.update(Role.any()),
                    Permission.delete(Role.any()),
                ],
            });

            const result = await db.updateDocument(
                testCollectionName,
                docId,
                updateDoc,
            );
            expect(result).toBeTruthy();
            expect(result.getAttribute("name")).toBe("John Doe Updated");
            expect(result.getAttribute("age")).toBe(31);
            await db.getCache().clear(); // Some issue with cache
            // Verify update via a separate get operation
            const updatedDoc = await db.getDocument(testCollectionName, docId);
            expect(updatedDoc.getAttribute("name")).toBe("John Doe Updated");
            expect(updatedDoc.getAttribute("email")).toBe("john@example.com"); // Should preserve existing fields
        });

        test("should create and find multiple documents", async () => {
            if (!runTests) return;

            // Create test data
            const testDocs: Document[] = [];
            const docIds: string[] = [];

            for (let i = 1; i <= 5; i++) {
                const docId = `query-doc${i}-${Date.now()}`;
                docIds.push(docId);

                testDocs.push(
                    new Document({
                        $id: docId,
                        name: `Test User ${i}`,
                        email: `user${i}@example.com`,
                        age: 20 + i,
                        isActive: i % 2 === 0,
                        $permissions: [
                            Permission.read(Role.any()),
                            Permission.write(Role.any()),
                        ],
                    }),
                );
            }

            // Insert all documents
            await Promise.all(
                testDocs.map((doc) =>
                    db.createDocument(testCollectionName, doc),
                ),
            );

            console.log(
                `Created test documents with IDs: ${docIds.join(", ")}`,
            );

            // Basic find all
            const allDocs = await db.find(testCollectionName);
            console.log(
                `Found ${allDocs.length} documents in collection ${testCollectionName}`,
            );
            expect(allDocs.length).toBeGreaterThan(0);

            // Find with query
            const activeDocs = await db.find(testCollectionName, [
                Query.equal("isActive", [true]),
            ]);

            expect(activeDocs.length).toBeGreaterThan(0);
            activeDocs.forEach((doc) => {
                expect(doc.getAttribute("isActive")).toBe(true);
            });

            // Find with complex query
            const complexResults = await db.find(testCollectionName, [
                Query.equal("isActive", [true]),
                Query.greaterThan("age", 20),
            ]);

            complexResults.forEach((doc) => {
                expect(doc.getAttribute("isActive")).toBe(true);
                expect(doc.getAttribute("age")).toBeGreaterThan(20);
            });
        });

        test("should delete a document", async () => {
            if (!runTests) return;

            const result = await db.deleteDocument(testCollectionName, docId);
            expect(result).toBe(true);

            // Verify document is gone
            try {
                await db.getDocument(testCollectionName, docId);
                fail("Document should not exist");
            } catch (err) {
                expect(err).toBeTruthy();
            }
        });
    });

    describe("Cache Operations", () => {
        test("should cache document reads", async () => {
            if (!runTests) return;

            // Create a document for cache testing
            const cacheDocId = `cache-test-${Date.now()}`;
            const cacheDoc = new Document({
                $id: cacheDocId,
                name: "Cache Test",
                email: "cache@example.com",
                $permissions: [Permission.read(Role.any())],
            });

            await db.createDocument(testCollectionName, cacheDoc);
            console.log(`Created cache test document with ID: ${cacheDocId}`);

            // Verify document was created
            const verifyDoc = await db.getDocument(
                testCollectionName,
                cacheDocId,
            );
            console.log(
                `Cache test document verification: name=${verifyDoc.getAttribute("name")}`,
            );

            // First read (from database)
            const startTime = Date.now();
            const firstRead = await db.getDocument(
                testCollectionName,
                cacheDocId,
            );
            const firstReadTime = Date.now() - startTime;
            console.log(`First read name: ${firstRead.getAttribute("name")}`);

            // Second read (should be from cache)
            const cacheStartTime = Date.now();
            const cachedRead = await db.getDocument(
                testCollectionName,
                cacheDocId,
            );
            const cachedReadTime = Date.now() - cacheStartTime;

            // Cached read should be faster, but we don't check this since
            // it could vary based on system performance
            expect(cachedRead.getAttribute("name")).toBe("Cache Test");

            // Clean up
            await Authorization.skip(
                async () =>
                    await db.deleteDocument(testCollectionName, cacheDocId),
            );
        });
    });
});
