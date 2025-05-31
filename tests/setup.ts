import { Client } from "pg";

/**
 * Setup script for PostgreSQL tests
 *
 * This script can be used to initialize a test database and required structures
 * before running tests.
 */
async function setupPostgresTestDB() {
    // Only run if PostgreSQL tests are enabled
    if (process.env.PG_TEST_CONNECTION !== "true") {
        console.log(
            "PostgreSQL test setup skipped. Set PG_TEST_CONNECTION=true to enable.",
        );
        return;
    }

    let client: Client | null = null;

    try {
        // Connect to PostgreSQL server
        client = new Client({
            host: process.env.PG_HOST || "localhost",
            port: parseInt(process.env.PG_PORT || "5432"),
            user: process.env.PG_USER || "postgres",
            password: process.env.PG_PASSWORD || "postgres",
            // Connect to default database to create test database
            database: "postgres",
        });

        await client.connect();

        const testDBName = process.env.PG_DATABASE || "test_db";

        // Check if database exists
        const dbResult = await client.query(
            `SELECT 1 FROM pg_database WHERE datname = $1`,
            [testDBName],
        );

        if (dbResult.rows.length === 0) {
            console.log(`Creating test database: ${testDBName}`);
            // Create test database
            await client.query(`CREATE DATABASE ${testDBName}`);
        } else {
            console.log(`Test database already exists: ${testDBName}`);
        }

        await client.end();

        // Connect to the test database
        client = new Client({
            host: process.env.PG_HOST || "localhost",
            port: parseInt(process.env.PG_PORT || "5432"),
            user: process.env.PG_USER || "postgres",
            password: process.env.PG_PASSWORD || "postgres",
            database: testDBName,
        });

        await client.connect();

        const testSchema = process.env.PG_SCHEMA || "public";

        // If using a custom schema, ensure it exists
        if (testSchema !== "public") {
            const schemaResult = await client.query(
                `SELECT 1 FROM information_schema.schemata WHERE schema_name = $1`,
                [testSchema],
            );

            if (schemaResult.rows.length === 0) {
                console.log(`Creating test schema: ${testSchema}`);
                await client.query(`CREATE SCHEMA ${testSchema}`);
            } else {
                console.log(`Test schema already exists: ${testSchema}`);
            }
        }

        console.log("PostgreSQL test setup completed successfully");
    } catch (err) {
        console.error("Error setting up PostgreSQL test database:", err);
    } finally {
        if (client) {
            await client.end();
        }
    }
}

// Run the setup if this script is executed directly
if (import.meta.url === `file://${process.argv[1]}`) {
    setupPostgresTestDB().catch(console.error);
}

export default setupPostgresTestDB;
