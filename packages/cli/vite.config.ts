import { defineConfig } from 'vitest/config';

export default defineConfig({
  build: {
    target: 'node24',
    sourcemap: true,
    minify: false,
    // Entry points share the CLI engine. Core stays external and is imported lazily by commands
    // that connect; embedded hosts can supply their own connection without loading it here.
    lib: { entry: { index: 'src/index.ts', bin: 'src/bin.ts', embedded: 'src/embedded.ts' }, formats: ['es'] },
    rollupOptions: {
      external: [
        /^node:/,
        '@cookiemonsterdev/kafka-core',
        '@cookiemonsterdev/kafka-config',
        '@cookiemonsterdev/kafka-studio',
      ],
      output: {
        entryFileNames: '[name].js',
      },
    },
  },
  test: {
    projects: [
      {
        test: {
          name: 'unit',
          // The tarball suite needs no broker (it packs and installs a real tarball), so it
          // stays in the unit project alongside everything else `pnpm test` runs.
          include: ['src/**/*.test.ts', 'test/*.test.ts', 'test/suites/tarball.test.ts'],
          environment: 'node',
        },
      },
      {
        test: {
          name: 'integration',
          include: ['test/suites/**/*.test.ts'],
          // Already covered by the unit project above (needs no broker), so it doesn't belong to
          // this project's `globalSetup` — same pattern as core's own vite.config.ts.
          exclude: ['test/suites/tarball.test.ts'],
          environment: 'node',
          globalSetup: ['./test/helpers/global-setup.ts'],
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
