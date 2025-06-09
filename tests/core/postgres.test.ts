import { PostgreDB, PostgreDBOptions } from "../../src/adapter/postgre";
import { Adapter } from "../../src/adapter/base";
import { Pool, PoolConfig, Client } from "pg";
import { Document } from "../../src/core/Document";
import { Query } from "../../src/core/query";
import { Database } from "../../src/core/database";
import { Authorization } from "../../src/security/authorization";
import { Role, Permission } from "../../src/index";
import { DB } from "../config"; // Assuming this config exists for test DB connection
import {
    DatabaseError,
    DuplicateException,
    InitializeError,
    TimeoutException,
    TruncateException,
} from "../../src/errors";


// Helper function to get adapter
async function getAdapter(options?: Partial<PostgreDBOptions>): Promise<PostgreDB> {
    const ssl = process.env["SSL"] === "true";
    const client = await new Pool({
        connectionString: DB,
        ssl: ssl ? { rejectUnauthorized: false } : undefined,
    }).connect();
    const defaultOptions: PostgreDBOptions = {
        connection: client,
        schema: "public",
    };
    const adapter = new PostgreDB({ ...defaultOptions, ...options });
    return adapter;
}

const runTests = process.env["SKIP_DB_TESTS"] !== "true";
const describeIf = runTests ? describe : describe.skip;

describeIf("PostgreDB Adapter", () => {
    let adapter: PostgreDB;
    const testSchema = `test_schema_${Date.now()}`;
    const baseCollectionName = `test_collection_base_${Date.now()}`;

    jest.setTimeout(90000);

    beforeAll(async () => {
        adapter = await getAdapter({ schema: testSchema });
        adapter.init();
        await adapter.ping();

        try {
            await adapter.create(testSchema);
        } catch (e: any) {
            if (e instanceof DuplicateException || (e.message && e.message.includes("already exists"))) {
                console.warn(`Schema ${testSchema} already exists. Attempting to drop and recreate.`);
                await adapter.drop(testSchema);
                await adapter.create(testSchema);
            } else {
                throw e;
            }
        }
        adapter.setDatabase(testSchema);
        adapter.setPrefix("testprefix");

        jest.spyOn(Authorization, "getRoles").mockReturnValue([Role.any().toString(), Role.user("mock_user").toString()]);
        jest.spyOn(Authorization, "getStatus").mockReturnValue(false); // Disable auth checks by default
    });

    afterAll(async () => {
        if (adapter && adapter.isInitialized()) {
            adapter.setDatabase("public");
            try {
                await adapter.drop(testSchema);
            } catch (error) {
                console.error(`Error dropping test schema ${testSchema}:`, error);
            }
            await adapter.close();
        }
        jest.restoreAllMocks();
    });

    describe("Initialization and Connection", () => {
        test("should initialize correctly", async () => {
            const newAdapter = await getAdapter({ schema: `init_test_${Date.now()}` });
            expect(newAdapter.isInitialized()).toBe(false);
            newAdapter.init();
            expect(newAdapter.isInitialized()).toBe(true);
            await newAdapter.close();
        });

        test("should throw error if initialized twice", async () => {
            const newAdapter = await getAdapter({ schema: `init_twice_${Date.now()}` });
            newAdapter.init();
            expect(() => newAdapter.init()).toThrow(InitializeError);
            await newAdapter.close();
        });

        test("should ping the database", async () => {
            await expect(adapter.ping()).resolves.not.toThrow();
        });

        test("should get a client from the pool", async () => {
            const client = await adapter.getClient();
            expect(client).toBeDefined();
            expect(typeof client.query).toBe("function");
            client.release();
        });

        test("should close the connection pool", async () => {
            const newAdapter = await getAdapter({ schema: `close_test_${Date.now()}` });
            newAdapter.init();
            await newAdapter.ping();
            await newAdapter.close();
            expect(newAdapter.isInitialized()).toBe(false);
            await expect(newAdapter.ping()).rejects.toThrow(DatabaseError);
        });

        test("should use an existing pool if provided", async () => {
            const existingPool = await new Pool({ connectionString: DB }).connect();
            const newAdapter = new PostgreDB({ connection: existingPool, schema: "public" });
            newAdapter.init();
            expect(newAdapter.isInitialized()).toBe(true);
            // @ts-ignore access private pool for test
            expect(newAdapter.pool).toBe(existingPool);
            await newAdapter.close(); // This will call pool.end()
            // await existingPool.end(); // Pool is ended by adapter.close()
        });
    });

    describe("Diagnostics", () => {
        test("should get diagnostics", async () => {
            const diagnostics = await adapter.getDiagnostics();
            expect(diagnostics).toHaveProperty("timestamp");
            expect(diagnostics).toHaveProperty("poolStatus");
            expect(diagnostics).toHaveProperty("queryPerformance");
            expect(diagnostics).toHaveProperty("databaseInfo");
            expect(diagnostics.databaseInfo.version).toBeDefined();
            expect(typeof diagnostics.databaseInfo.version).toBe("string");
            expect(diagnostics.adapter.type).toBe("postgresql");
            expect(diagnostics.adapter.database).toBe(testSchema);
        });
    });

    describe("Error Processing", () => {
        test("should process timeout error", () => {
            const pgError = { code: "57014", message: "Query cancelled" };
            const processedError = adapter.processException(pgError);
            expect(processedError).toBeInstanceOf(TimeoutException);
        });

        test("should process duplicate table error", () => {
            const pgError = { code: "42P07", message: "Table already exists" };
            const processedError = adapter.processException(pgError);
            expect(processedError).toBeInstanceOf(DuplicateException);
        });

        test("should process duplicate column error", () => {
            const pgError = { code: "42701", message: "Column already exists" };
            const processedError = adapter.processException(pgError);
            expect(processedError).toBeInstanceOf(DuplicateException);
        });

        test("should process duplicate row error", () => {
            const pgError = { code: "23505", message: "Unique constraint violation" };
            const processedError = adapter.processException(pgError);
            expect(processedError).toBeInstanceOf(DuplicateException);
        });

        test("should process truncate error", () => {
            const pgError = { code: "22001", message: "Value too long for type character varying(10)" };
            const processedError = adapter.processException(pgError);
            expect(processedError).toBeInstanceOf(TruncateException);
        });

        test("should return original error if not a known PG error code", () => {
            const genericError = new Error("Generic error");
            const processedError = adapter.processException(genericError);
            expect(processedError).toBe(genericError);
        });
    });

    describe("SQL Generation Helpers", () => {
        test("getSQLType should return correct PostgreSQL types", () => {
            expect(adapter.getSQLType(Database.VAR_STRING, 100)).toBe("VARCHAR(100)");
            expect(adapter.getSQLType(Database.VAR_STRING, 2000000)).toBe("TEXT");
            expect(adapter.getSQLType(Database.VAR_INTEGER, 4)).toBe("INTEGER");
            expect(adapter.getSQLType(Database.VAR_INTEGER, 8)).toBe("BIGINT");
            expect(adapter.getSQLType(Database.VAR_FLOAT, 8)).toBe("DOUBLE PRECISION");
            expect(adapter.getSQLType(Database.VAR_BOOLEAN, 1)).toBe("BOOLEAN");
            expect(adapter.getSQLType(Database.VAR_DATETIME, 0)).toBe("TIMESTAMP(3)");
            expect(adapter.getSQLType(Database.VAR_STRING, 50, true, true)).toBe("JSONB");
            expect(adapter.getSQLType(Database.VAR_RELATIONSHIP, 0)).toBe("VARCHAR(255)");
        });
    });

    describe("Schema and Collection Operations", () => {
        const newSchemaName = `schema_op_test_${Date.now()}`;
        const newCollectionName = `coll_op_test_${Date.now()}`;

        afterEach(async () => {
            try {
                await adapter.dropCollection(newCollectionName);
            } catch (e) { /* ignore */ }
            try {
                await adapter.drop(newSchemaName);
            } catch (e) { /* ignore */ }
        });

        test("should create and check existence of a schema", async () => {
            let exists = await adapter.exists(newSchemaName);
            expect(exists).toBe(false);
            await adapter.create(newSchemaName);
            exists = await adapter.exists(newSchemaName);
            expect(exists).toBe(true);
        });

        test("should drop a schema", async () => {
            await adapter.create(newSchemaName);
            let exists = await adapter.exists(newSchemaName);
            expect(exists).toBe(true);
            await adapter.drop(newSchemaName);
            exists = await adapter.exists(newSchemaName);
            expect(exists).toBe(false);
        });

        test("should create a collection", async () => {
            const attributes = [
                new Document({ $id: "name", key: "name", type: "string", size: 100 }),
                new Document({ $id: "age", key: "age", type: "integer" }),
            ];
            const indexes = [
                new Document({ $id: "name_idx", type: Database.INDEX_KEY, attributes: ["name"] }),
            ];
            const result = await adapter.createCollection(newCollectionName, attributes, indexes);
            expect(result).toBe(true);
            const tableName = `${adapter.getPrefix()}_${newCollectionName}`;
            const exists = await adapter.exists(testSchema, tableName);
            expect(exists).toBe(true);
        });

        test("should check collection existence", async () => {
            await adapter.createCollection(newCollectionName, [new Document({ $id: "field", key: "field", type: "string", size: 10 })]);
            const tableName = `${adapter.getPrefix()}_${newCollectionName}`;
            const exists = await adapter.exists(testSchema, tableName);
            expect(exists).toBe(true);
            const notExists = await adapter.exists(testSchema, `${adapter.getPrefix()}_nonexistent_${Date.now()}`);
            expect(notExists).toBe(false);
        });

        test("should drop a collection", async () => {
            await adapter.createCollection(newCollectionName, [new Document({ $id: "field", key: "field", type: "string", size: 10 })]);
            const tableName = `${adapter.getPrefix()}_${newCollectionName}`;
            let exists = await adapter.exists(testSchema, tableName);
            expect(exists).toBe(true);
            const result = await adapter.dropCollection(newCollectionName);
            expect(result).toBe(true);
            exists = await adapter.exists(testSchema, tableName);
            expect(exists).toBe(false);
        });
    });

    describe("Attribute Operations", () => {
        const attrCollection = `attr_coll_${Date.now()}`;
        beforeAll(async () => {
            await adapter.createCollection(attrCollection, [
                new Document({ $id: "initial_attr", key: "initial_attr", type: "string", size: 50 })
            ]);
        });
        afterAll(async () => {
            await adapter.dropCollection(attrCollection);
        });

        test("should create an attribute", async () => {
            const result = await adapter.createAttribute(attrCollection, "new_attr", Database.VAR_INTEGER, 4);
            expect(result).toBe(true);
        });

        test("should update an attribute (type change and rename)", async () => {
            await adapter.createAttribute(attrCollection, "attr_to_update", Database.VAR_STRING, 20);
            let result = await adapter.updateAttribute(attrCollection, "attr_to_update", Database.VAR_INTEGER, 4);
            expect(result).toBe(true);
            result = await adapter.updateAttribute(attrCollection, "attr_to_update", Database.VAR_INTEGER, 4, true, false, "renamed_attr");
            expect(result).toBe(true);
        });

        test("should rename an attribute", async () => {
            await adapter.createAttribute(attrCollection, "attr_to_rename", Database.VAR_STRING, 10);
            const result = await adapter.renameAttribute(attrCollection, "attr_to_rename", "attr_renamed_explicitly");
            expect(result).toBe(true);
        });

        test("should delete an attribute", async () => {
            await adapter.createAttribute(attrCollection, "attr_to_delete", Database.VAR_BOOLEAN, 1);
            const result = await adapter.deleteAttribute(attrCollection, "attr_to_delete");
            expect(result).toBe(true);
        });
    });

    describe("Index Operations", () => {
        const idxCollection = `idx_coll_${Date.now()}`;
        const idxAttrName = "indexed_attr";
        const idxName = "my_index";

        beforeAll(async () => {
            await adapter.createCollection(idxCollection, [
                new Document({ $id: idxAttrName, key: idxAttrName, type: "string", size: 50 })
            ]);
        });
        afterAll(async () => {
            await adapter.dropCollection(idxCollection);
        });

        test("should create an index", async () => {
            const result = await adapter.createIndex(idxCollection, idxName, Database.INDEX_KEY, [idxAttrName]);
            expect(result).toBe(true);
        });

        test("should rename an index", async () => {
            const oldIdxName = `old_idx_${Date.now()}`;
            const newIdxName = `new_idx_${Date.now()}`;
            await adapter.createIndex(idxCollection, oldIdxName, Database.INDEX_KEY, [idxAttrName]);
            const result = await adapter.renameIndex(idxCollection, oldIdxName, newIdxName);
            expect(result).toBe(true);
            await adapter.deleteIndex(idxCollection, newIdxName);
        });

        test("should delete an index", async () => {
            const tempIdxName = `temp_idx_${Date.now()}`;
            await adapter.createIndex(idxCollection, tempIdxName, Database.INDEX_KEY, [idxAttrName]);
            const result = await adapter.deleteIndex(idxCollection, tempIdxName);
            expect(result).toBe(true);
        });
    });

    describe("Transaction Management", () => {
        const txCollection = `tx_coll_${Date.now()}`;
        const docIdBase = `tx_doc_${Date.now()}`;

        beforeEach(async () => {
            await adapter.createCollection(txCollection, [
                new Document({ $id: "name", key: "name", type: "string", size: 100 })
            ]);
        });
        afterEach(async () => {
            await adapter.dropCollection(txCollection);
        });

        test("should commit a transaction using withTransaction", async () => {
            const docId = `${docIdBase}_commit`;
            await adapter.withTransaction(async (client) => { // client is optional here, adapter handles it
                const doc = new Document({ $id: docId, name: "TX Commit Test" });
                await adapter.createDocument(txCollection, doc);
            });
            const fetchedDoc = await adapter.getDocument(txCollection, docId);
            expect(fetchedDoc.getAttribute("name")).toBe("TX Commit Test");
        });

        test("should rollback a transaction on error using withTransaction", async () => {
            const docId = `${docIdBase}_rollback`;
            try {
                await adapter.withTransaction(async (client) => {
                    const doc = new Document({ $id: docId, name: "TX Rollback Test" });
                    await adapter.createDocument(txCollection, doc);
                    throw new Error("Intentional error for rollback");
                });
            } catch (e: any) {
                expect(e.message).toBe("Intentional error for rollback");
            }
            await expect(adapter.getDocument(txCollection, docId)).rejects.toThrow();
        });

    });

    describe("Document Operations", () => {
        const docCollection = `doc_op_coll_${Date.now()}`;
        const doc1Id = `doc1_${Date.now()}`;
        const doc1 = new Document({
            $id: doc1Id,
            name: "Doc One",
            value: 10,
            tags: ["a", "b"],
            isActive: true,
            profile: { city: "NY", country: "USA" },
            $permissions: [Permission.read(Role.any())]
        });

        beforeAll(async () => {
            await adapter.createCollection(docCollection, [
                new Document({ $id: "name", key: "name", type: "string", size: 100 }),
                new Document({ $id: "value", key: "value", type: "integer" }),
                new Document({ $id: "tags", key: "tags", type: "string", array: true, size: 50 }),
                new Document({ $id: "isActive", key: "isActive", type: "boolean" }),
                new Document({ $id: "profile", key: "profile", type: "string", size: 1000, filters: ["json"] }),
            ]);
            await adapter.createDocument(docCollection, doc1);
        });

        afterAll(async () => {
            await adapter.dropCollection(docCollection);
        });

        test("should get a document", async () => {
            const fetched = await adapter.getDocument(docCollection, doc1Id);
            expect(fetched).toBeInstanceOf(Document);
            expect(fetched.getId()).toBe(doc1Id);
            expect(fetched.getAttribute("name")).toBe("Doc One");
            expect(fetched.getAttribute("value")).toBe(10);
            expect(fetched.getAttribute("tags")).toEqual(["a", "b"]);
            expect(fetched.getAttribute("isActive")).toBe(true);
            expect(fetched.getAttribute("profile")).toEqual({ city: "NY", country: "USA" });
        });

        test("should update a document", async () => {
            const updatedDoc = new Document({
                $id: doc1Id,
                name: "Doc One Updated",
                value: 20,
                isActive: false,
                profile: { city: "LA" },
                $permissions: [Permission.read(Role.any()), Permission.update(Role.any())]
            });
            const originalDoc = await adapter.getDocument(docCollection, doc1Id);
            updatedDoc.setAttribute("$internalId", originalDoc.getInternalId());

            const result = await adapter.updateDocument(docCollection, doc1Id, updatedDoc);
            expect(result.getAttribute("name")).toBe("Doc One Updated");

            const fetched = await adapter.getDocument(docCollection, doc1Id);
            expect(fetched.getAttribute("name")).toBe("Doc One Updated");
            expect(fetched.getAttribute("value")).toBe(20);
            expect(fetched.getAttribute("isActive")).toBe(false);
            expect(fetched.getAttribute("profile")).toEqual({ city: "LA" }); // JSON should merge/replace
        });

        test("should find documents", async () => {
            const results = await adapter.find(docCollection, [Query.equal("name", ["Doc One Updated"])]);
            expect(results.length).toBeGreaterThanOrEqual(1);
            expect(results[0]?.getAttribute("name")).toBe("Doc One Updated");
        });

        test("should count documents", async () => {
            const count = await adapter.count(docCollection, [Query.equal("isActive", [false])]);
            expect(count).toBeGreaterThanOrEqual(1);
        });

        test("should sum document attribute", async () => {
            const sum = await adapter.sum(docCollection, "value", [Query.equal("isActive", [false])]);
            expect(sum).toBe(20);
        });

        test("should increase document attribute", async () => {
            const success = await adapter.increaseDocumentAttribute(docCollection, doc1Id, "value", 5, new Date().toISOString());
            expect(success).toBe(true);
            const fetched = await adapter.getDocument(docCollection, doc1Id);
            expect(fetched.getAttribute("value")).toBe(25);
        });

        test("should create multiple documents", async () => {
            const doc2Id = `doc2_${Date.now()}`;
            const doc3Id = `doc3_${Date.now()}`;
            const docsToCreate = [
                new Document({ $id: doc2Id, name: "Doc Two", value: 200, $permissions: [Permission.read(Role.any())] }),
                new Document({ $id: doc3Id, name: "Doc Three", value: 300, $permissions: [Permission.read(Role.any())] }),
            ];
            const createdDocs = await adapter.createDocuments(docCollection, docsToCreate);
            expect(createdDocs.length).toBe(2);

            const fetchedDoc2 = await adapter.getDocument(docCollection, doc2Id);
            expect(fetchedDoc2.getAttribute("name")).toBe("Doc Two");
        });

        test("should update multiple documents", async () => {
            const docUp1Id = `docUp1_${Date.now()}`;
            const docUp2Id = `docUp2_${Date.now()}`;
            const docsToUpdate = [
                new Document({ $id: docUp1Id, name: "UpdateMe1", value: 1, $permissions: [Permission.read(Role.any())] }),
                new Document({ $id: docUp2Id, name: "UpdateMe2", value: 2, $permissions: [Permission.read(Role.any())] }),
            ];
            await adapter.createDocuments(docCollection, docsToUpdate);

            const updates = new Document({ value: 99, $permissions: [Permission.read(Role.any()), Permission.update(Role.any())] });
            const affectedCount = await adapter.updateDocuments(docCollection, updates, docsToUpdate);
            expect(affectedCount).toBe(2);

            const fetched1 = await adapter.getDocument(docCollection, docUp1Id);
            expect(fetched1.getAttribute("value")).toBe(99);
        });

        test("should delete multiple documents", async () => {
            const docDel1Id = `docDel1_${Date.now()}`;
            const docDel2Id = `docDel2_${Date.now()}`;
            const docsToDelete = [
                new Document({ $id: docDel1Id, name: "DeleteMe1", value: 1, $permissions: [Permission.read(Role.any())] }),
                new Document({ $id: docDel2Id, name: "DeleteMe2", value: 2, $permissions: [Permission.read(Role.any())] }),
            ];
            await adapter.createDocuments(docCollection, docsToDelete);

            const deletedCount = await adapter.deleteDocuments(docCollection, [docDel1Id, docDel2Id]);
            expect(deletedCount).toBe(2);
            await expect(adapter.getDocument(docCollection, docDel1Id)).rejects.toThrow();
        });

        test("should delete a document", async () => { // This should be last in this describe block for doc1Id
            const result = await adapter.deleteDocument(docCollection, doc1Id);
            expect(result).toBe(true);
            await expect(adapter.getDocument(docCollection, doc1Id)).rejects.toThrow();
        });
    });

    describe("Support Flags", () => {
        test("should return correct support flags", () => {
            expect(adapter.getSupportForIndex()).toBe(true);
            expect(adapter.getSupportForUniqueIndex()).toBe(true);
            expect(adapter.getSupportForFulltextIndex()).toBe(true);
            expect(adapter.getSupportForFulltextWildcardIndex()).toBe(false);
            expect(adapter.getSupportForCasting()).toBe(false);
            expect(adapter.getSupportForTimeouts()).toBe(true);
            expect(adapter.getSupportForJSONOverlaps()).toBe(false);
            expect(adapter.getSupportForSchemaAttributes()).toBe(false);
            expect(adapter.getSupportForUpserts()).toBe(false);
            expect(adapter.getSupportForCastIndexArray()).toBe(false);
        });

        test("getMinDateTime should return a valid date", () => {
            expect(adapter.getMinDateTime()).toEqual(new Date("0001-01-01 00:00:00"));
        });

        test("getLikeOperator should return ILIKE", () => {
            expect(adapter.getLikeOperator()).toBe("ILIKE");
        });
    });

    describe("Query Generation and Execution", () => {
        const queryCollection = `query_coll_${Date.now()}`;
        const qDocs = [
            new Document({ $id: "q1", name: "Alice", age: 30, city: "New York", isActive: true, tags: ["dev", "js"], $permissions: [Permission.read(Role.any())] }),
            new Document({ $id: "q2", name: "Bob", age: 24, city: "London", isActive: false, tags: ["qa", "py"], $permissions: [Permission.read(Role.any())] }),
            new Document({ $id: "q3", name: "Charlie", age: 35, city: "New York", isActive: true, tags: ["dev", "ts"], $permissions: [Permission.read(Role.any())] }),
            new Document({ $id: "q4", name: "Diana", age: 28, city: "Paris", isActive: true, tags: ["ux", "css"], $permissions: [Permission.read(Role.any())] }),
        ];

        beforeAll(async () => {
            await adapter.createCollection(queryCollection, [
                new Document({ $id: "name", key: "name", type: "string", size: 100 }),
                new Document({ $id: "age", key: "age", type: "integer" }),
                new Document({ $id: "city", key: "city", type: "string", size: 50 }),
                new Document({ $id: "isActive", key: "isActive", type: "boolean" }),
                new Document({ $id: "tags", key: "tags", type: "string", array: true, size: 30 }),
            ]);
            await adapter.createDocuments(queryCollection, qDocs);
        });
        afterAll(async () => {
            await adapter.dropCollection(queryCollection);
        });

        test("find with multiple queries and ordering", async () => {
            const queries = [
                Query.equal("city", ["New York"]),
                Query.greaterThan("age", 25)
            ];
            const results = await adapter.find(queryCollection, queries, 10, 0, ["age"], [Database.ORDER_DESC]);
            expect(results.length).toBe(2);
            expect(results[1]?.getAttribute("name")).toBe("Alice");
            expect(results[0]?.getAttribute("name")).toBe("Charlie");
        });

        test("find with cursor pagination (after)", async () => {
            const page1 = await adapter.find(queryCollection, [], 2, 0, ["name"], [Database.ORDER_ASC]);
            expect(page1.length).toBe(2);
            expect(page1[0]?.getId()).toBe("q1"); // Alice
            const cursorDocPage1 = page1[1]; // Bob

            const page2 = await adapter.find(queryCollection, [], 2, 0, ["name"], [Database.ORDER_ASC], cursorDocPage1, Database.CURSOR_AFTER);
            expect(page2.length).toBe(2);
            expect(page2[0]?.getId()).toBe("q3"); // Charlie
        });

        test("find with cursor pagination (before)", async () => {
            const allSortedDesc = await adapter.find(queryCollection, [], 10, 0, ["name"], [Database.ORDER_DESC]); // Diana, Charlie, Bob, Alice
            const cursorDoc = allSortedDesc[1]; // Charlie (index 1 when sorted DESC)

            const pageBefore = await adapter.find(queryCollection, [], 2, 0, ["name"], [Database.ORDER_ASC], cursorDoc, Database.CURSOR_BEFORE);
            expect(pageBefore.length).toBe(2);
            expect(pageBefore[0]?.getId()).toBe("q1"); // Alice
            expect(pageBefore[1]?.getId()).toBe("q2"); // Bob
        });

        test("find with $id (uid) in ordering and cursor", async () => {
            const allSortedById = await adapter.find(queryCollection, [], 10, 0, ["$id"], [Database.ORDER_ASC]);
            const cursorDoc = allSortedById[1];

            const results = await adapter.find(queryCollection, [], 2, 0, ["$id"], [Database.ORDER_ASC], cursorDoc, Database.CURSOR_AFTER);
            expect(results.length).toBe(2);
            expect(results[0]?.getId()).toBe(allSortedById[2]?.getId());
        });

        test("find with $internalId (_id) in ordering and cursor", async () => {
            const allSorted = await adapter.find(queryCollection, [], 10, 0, ["$internalId"], [Database.ORDER_ASC]);
            const cursorDoc = allSorted[1];

            const results = await adapter.find(queryCollection, [], 2, 0, ["$internalId"], [Database.ORDER_ASC], cursorDoc, Database.CURSOR_AFTER);
            expect(results.length).toBe(2);
            expect(results[0]?.getInternalId()).toBeGreaterThan(Number(cursorDoc!.getInternalId()!));
        });
    });
});