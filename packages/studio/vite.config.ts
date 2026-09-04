import { defineConfig } from 'vitest/config';

export default defineConfig({
  build: {
    target: 'node24',
    sourcemap: true,
    minify: false,
    // Bundled to a single file per entry, matching the CLI's build: cheap to parse on every
    // invocation, with core/config kept external so they resolve to the workspace's own copies.
    lib: { entry: { index: 'src/index.ts', bin: 'src/bin.ts' }, formats: ['es'] },
    rollupOptions: {
      external: [/^node:/, '@cookiemonsterdev/kafka-core', '@cookiemonsterdev/kafka-config'],
      output: {
        entryFileNames: '[name].js',
      },
    },
  },
  test: {
    // Only the "integration" project is currently empty (no test/suites/** yet); this is a
    // workspace-level, not per-project, vitest option. Remove once real integration tests exist
    // so a genuinely empty run fails again, same as everywhere else.
    passWithNoTests: true,
    projects: [
      {
        test: {
          name: 'unit',
          include: ['src/**/*.test.ts', 'test/*.test.ts', 'test/suites/tarball.test.ts'],
          environment: 'node',
        },
      },
      {
        test: {
          name: 'integration',
          include: ['test/suites/**/*.test.ts'],
          exclude: ['test/suites/tarball.test.ts'],
          environment: 'node',
          testTimeout: 30_000,
          hookTimeout: 60_000,
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
