import { defineConfig } from 'vitest/config';

export default defineConfig({
  build: {
    target: 'node24',
    sourcemap: true,
    minify: false,
    lib: { entry: { index: 'src/index.ts' }, formats: ['es'] },
    rollupOptions: {
      external: [/^node:/, 'kerberos', 'lz4-lite', 'snappyjs', '@cookiemonsterdev/kafka-config'],
      output: {
        // 1:1 with src, so the emitted .js tree matches the tsc-emitted .d.ts tree
        preserveModules: true,
        preserveModulesRoot: 'src',
        entryFileNames: '[name].js',
      },
    },
  },
  test: {
    projects: [
      {
        test: {
          name: 'unit',
          // The published-file-count suite needs no broker (it only builds and packs the
          // package), so it stays in the unit project alongside everything else `pnpm test` runs.
          include: ['src/**/*.test.ts', 'test/helpers/**/*.test.ts', 'test/suites/publishable-file-count.test.ts'],
          environment: 'node',
        },
      },
      {
        test: {
          name: 'integration',
          include: ['test/suites/**/*.test.ts'],
          // Already covered by the unit project above — it needs no broker, so it doesn't belong
          // to this project's `globalSetup`, and running it under both would just be redundant.
          exclude: ['test/suites/publishable-file-count.test.ts'],
          environment: 'node',
          globalSetup: ['./test/helpers/global-setup.ts'],
          testTimeout: 30_000,
          hookTimeout: 60_000,
          maxWorkers: 4,
          fileParallelism: true,
          retry: Number(process.env.TEST_RETRIES ?? 0),
        },
      },
    ],
    coverage: {
      provider: 'v8',
      reporter: ['text', 'lcov'],
      thresholds: {
        // Raise these as coverage grows.
      },
    },
  },
});
