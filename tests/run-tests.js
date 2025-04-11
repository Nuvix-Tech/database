#!/usr/bin/env node

/**
 * Unified test runner for the database library
 *
 * This script runs the database tests with the proper configuration.
 * By default it runs core tests, but can be configured to run adapter-specific tests.
 *
 * Usage:
 *   npm run test                  - Run core tests only
 *   npm run test:pg               - Run PostgreSQL adapter tests
 *   npm run test -- --all         - Run all tests
 *   npm run test -- --testPathPattern=*.test.ts  - Run tests matching pattern
 */

const { spawn } = require("child_process");
const path = require("path");
const fs = require("fs");

// Read DB connection string from config.ts
let connectionString = "";
try {
    const configContent = fs.readFileSync(
        path.join(__dirname, "config.ts"),
        "utf8",
    );
    const match = configContent.match(/DB\s*=\s*`(.+?)`/);
    if (match && match[1]) {
        connectionString = match[1];
    }
} catch (err) {
    console.error("Error reading config.ts:", err);
    process.exit(1);
}

if (!connectionString) {
    console.error("Could not find DB connection string in config.ts");
    process.exit(1);
}

// Default configuration
const defaultConfig = {
    SKIP_DB_TESTS: "false",
    DB: connectionString,
    SSL: "true",
};

// Parse command line arguments
const args = process.argv.slice(2);
const envOverrides = {};
let jestArgs = [];
let runAdapterTests = false;
let runAllTests = false;

for (let i = 0; i < args.length; i++) {
    const arg = args[i];

    if (arg === "--pg") {
        runAdapterTests = true;
        envOverrides.PG_TEST_CONNECTION = "true";
    } else if (arg === "--all") {
        runAllTests = true;
        envOverrides.PG_TEST_CONNECTION = "true";
    } else if (arg.startsWith("--env.")) {
        const [_, key, value] = arg.match(/--env\.([^=]+)=(.+)/);
        envOverrides[key] = value;
    } else {
        jestArgs.push(arg);
    }
}

// Set default Jest args if none provided
if (jestArgs.length === 0) {
    if (runAdapterTests) {
        jestArgs = [
            "--testPathPattern",
            "(postgre|database-pg).*\\.test\\.ts$",
        ];
    } else if (runAllTests) {
        jestArgs = [];
    } else {
        // Core tests by default
        jestArgs = ["--testPathPattern", "database-core\\.test\\.ts$"];
    }
}

// Merge environment variables
const env = {
    ...process.env,
    ...defaultConfig,
    ...envOverrides,
};

// Print test configuration
console.log("Running database tests...");
console.log("Test configuration:");
Object.entries({
    ...defaultConfig,
    ...envOverrides,
}).forEach(([key, value]) => {
    if (key === "DB") {
        // Mask the password in the connection string for security
        console.log(
            `  ${key}=${value.replace(/\/\/[^:]+:([^@]+)@/, "//****:****@")}`,
        );
    } else {
        console.log(`  ${key}=${value}`);
    }
});

console.log("Debug information:");
console.log("  Connection string loaded from config.ts");
if (runAdapterTests) {
    console.log("  Running PostgreSQL adapter tests");
} else if (runAllTests) {
    console.log("  Running all tests");
} else {
    console.log("  Running core tests only");
}
console.log("  Workspace directory:", process.cwd());
console.log("  Running Jest with arguments:", jestArgs.join(" "));

// Run tests
const jestProcess = spawn("npx", ["jest", ...jestArgs], {
    stdio: "inherit",
    env,
});

jestProcess.on("close", (code) => {
    process.exit(code);
});
