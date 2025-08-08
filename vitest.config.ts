import { defineConfig } from 'vitest/config';
import tsconfigPaths from 'vite-tsconfig-paths';
import { fileURLToPath } from 'node:url';

export default defineConfig({
    plugins: [tsconfigPaths()],
    resolve: {
        alias: [
            { find: /^@core\/(.*)\.js$/, replacement: fileURLToPath(new URL('./src/core/$1.ts', import.meta.url)) },
            { find: /^@adapters\/(.*)\.js$/, replacement: fileURLToPath(new URL('./src/adapters/$1.ts', import.meta.url)) },
            { find: /^@utils\/(.*)\.js$/, replacement: fileURLToPath(new URL('./src/utils/$1.ts', import.meta.url)) },
            { find: /^@validators\/(.*)\.js$/, replacement: fileURLToPath(new URL('./src/validators/$1.ts', import.meta.url)) },
            { find: /^@errors\/(.*)\.js$/, replacement: fileURLToPath(new URL('./src/errors/$1.ts', import.meta.url)) },
            { find: /^@hooks\/(.*)\.js$/, replacement: fileURLToPath(new URL('./src/hooks/$1.ts', import.meta.url)) },
            // allow imports without .js as well
            { find: /^@core\/(.*)$/, replacement: fileURLToPath(new URL('./src/core/$1.ts', import.meta.url)) },
            { find: /^@adapters\/(.*)$/, replacement: fileURLToPath(new URL('./src/adapters/$1.ts', import.meta.url)) },
            { find: /^@utils\/(.*)$/, replacement: fileURLToPath(new URL('./src/utils/$1.ts', import.meta.url)) },
            { find: /^@validators\/(.*)$/, replacement: fileURLToPath(new URL('./src/validators/$1.ts', import.meta.url)) },
            { find: /^@errors\/(.*)$/, replacement: fileURLToPath(new URL('./src/errors/$1.ts', import.meta.url)) },
            { find: /^@hooks\/(.*)$/, replacement: fileURLToPath(new URL('./src/hooks/$1.ts', import.meta.url)) },
        ],
    },
    test: {
        globals: true,
        environment: 'node',
        include: ['tests/**/*.test.ts'],
        passWithNoTests: false,
        reporters: ['default'],
        coverage: {
            reporter: ['text', 'html'],
        },
    },
});
