import { PostgreDB } from "../src/adapter/postgre";
import { Adapter } from "../src/adapter/base";
import { DB } from "./config";

/**
 * Adapter test utility functions
 */

/**
 * Creates and initializes a database adapter for testing
 * @returns An initialized adapter instance
 */
export function createTestAdapter(): Adapter {
    // Currently hardcoded to PostgreSQL, can be extended to support other adapters
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

/**
 * Checks if adapter tests should be executed based on environment variables
 * @returns True if adapter tests should run
 */
export function shouldRunAdapterTests(): boolean {
    return process.env["PG_TEST_CONNECTION"] === "true";
}

/**
 * Creates a unique test collection name
 * @param prefix Optional prefix for the collection name
 * @returns A unique collection name
 */
export function getTestCollectionName(prefix: string = "test"): string {
    return `${prefix}_${Date.now()}`;
}

/**
 * Logs a message if adapter tests are being skipped
 */
export function logSkippedTests(): void {
    console.log("Skipping adapter tests. Set PG_TEST_CONNECTION=true to run.");
}
