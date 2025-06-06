import typescript from "@rollup/plugin-typescript";
import resolve from "@rollup/plugin-node-resolve";
import commonjs from "@rollup/plugin-commonjs";
import json from "@rollup/plugin-json";
import dts from "rollup-plugin-dts";

// Common external dependencies that shouldn't be bundled
const external = [
    "chalk",
    "cli-highlight",
    "reflect-metadata",
    "mysql2",
    "pg",
    "ioredis",
];

// Base configuration
const baseConfig = {
    input: "src/index.ts",
    external,
    plugins: [
        resolve({
            preferBuiltins: true,
            exportConditions: ["node"],
        }),
        commonjs({
            include: ["node_modules/**"],
        }),
        json(),
        typescript({
            tsconfig: "./tsconfig.json",
            declaration: false, // We'll handle declarations separately
            declarationMap: false,
            sourceMap: true,
            exclude: ["**/*.test.ts", "**/*.spec.ts", "tests/**/*"],
        }),
    ],
};

// ESM build
const esmConfig = {
    ...baseConfig,
    output: {
        file: "dist/index.esm.js",
        format: "esm",
        sourcemap: true,
        exports: "named",
    },
};

// CJS build
const cjsConfig = {
    ...baseConfig,
    output: {
        file: "dist/index.cjs.js",
        format: "cjs",
        sourcemap: true,
        exports: "named",
    },
};

// Type definitions build
const dtsConfig = {
    input: "src/index.ts",
    external,
    plugins: [
        dts({
            tsconfig: "./tsconfig.json",
        }),
    ],
    output: {
        file: "dist/index.d.ts",
        format: "esm",
    },
};

export default [esmConfig, cjsConfig, dtsConfig];
