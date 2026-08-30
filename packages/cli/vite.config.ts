import { defineConfig } from 'vitest/config';

export default defineConfig({
  build: {
    target: 'node24',
    sourcemap: true,
    minify: false,
    // Bundled to a single file, unlike core/kafka-config's preserveModules build: the CLI's own
    // code should cost one cheap parse on every invocation, including --help/--version, and core
    // (kept external below) is meant to be await-imported only inside commands that connect.
    lib: { entry: { index: 'src/index.ts' }, formats: ['es'] },
    rollupOptions: {
      external: [/^node:/, '@cookiemonsterdev/kafka-core'],
      output: {
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
