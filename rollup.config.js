import path from 'path';
import typescript from '@rollup/plugin-typescript';
import { nodeResolve } from '@rollup/plugin-node-resolve';
import commonjs from '@rollup/plugin-commonjs';
import dts from 'rollup-plugin-dts';

const input = 'src/index.ts';
const outDir = 'dist';

export default [
    // ESM build
    {
        input,
        output: {
            file: path.join(outDir, 'index.esm.js'),
            format: 'esm',
            sourcemap: true,
        },
        plugins: [
            nodeResolve(),
            commonjs(),
            typescript({ tsconfig: './tsconfig.json', declaration: false }),
        ],
        external: [
            'pg', '@nuvix/cache', 'chalk', 'stream', 'node:crypto', 'vite-tsconfig-paths', 'prettier', 'typescript', 'vitest', '@types/node', '@types/pg'
        ],
    },
    // CJS build
    {
        input,
        output: {
            file: path.join(outDir, 'index.cjs.js'),
            format: 'cjs',
            sourcemap: true,
            exports: 'named',
        },
        plugins: [
            nodeResolve(),
            commonjs(),
            typescript({ tsconfig: './tsconfig.json', declaration: false }),
        ],
        external: [
            'pg', '@nuvix/cache', 'chalk', 'stream', 'node:crypto', 'vite-tsconfig-paths', 'prettier', 'typescript', 'vitest', '@types/node', '@types/pg'
        ],
    },
    // Type declarations
    {
        input,
        output: {
            file: path.join(outDir, 'index.d.ts'),
            format: 'es',
        },
        plugins: [dts()],
    },
];
