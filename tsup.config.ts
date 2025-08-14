import { defineConfig } from "tsup";

export default defineConfig({
  entry: ["src/index.ts", 'src/config/types.ts', 'src/cli/generate-types.ts'],
  format: ["cjs", "esm"],
  dts: false,
  sourcemap: true,
  clean: true,
  outDir: "dist",
  noExternal: [],
  splitting: false,
  minify: false,
  target: "es2024",
  shims: true,
  bundle: true,
  skipNodeModulesBundle: true,
  tsconfig: "./tsconfig.json",
});
