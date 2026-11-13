import { defineConfig } from 'vitest/config';

export default defineConfig({
  build: {
    target: 'node24',
    // Maps embedding the TS source were about 40% of the install size; unminified output stays debuggable without them.
    sourcemap: false,
    minify: false,
    lib: { entry: { index: 'src/index.ts' }, formats: ['es'] },
    rollupOptions: {
      external: [/^node:/],
      output: {
        // 1:1 with src, so the emitted .js tree matches the tsc-emitted .d.ts tree
        preserveModules: true,
        preserveModulesRoot: 'src',
        entryFileNames: '[name].js',
      },
    },
  },
  test: {
    include: ['src/**/*.test.ts', 'test/suites/**/*.test.ts'],
    environment: 'node',
    coverage: {
      provider: 'v8',
      reporter: ['text', 'lcov'],
      thresholds: {
        // Raise these as coverage grows.
      },
    },
  },
});
