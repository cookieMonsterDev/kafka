import { defineConfig } from 'vitest/config';

export default defineConfig({
  build: {
    target: 'node24',
    sourcemap: true,
    minify: false,
    // Bundled to a single file per entry, matching the CLI's build: cheap to parse on every
    // invocation, with core/config kept external so they resolve to the workspace's own copies.
    // `vite` is external too — `server/dev.ts` only imports it lazily, behind KAFKA_STUDIO_DEV,
    // and it must never be pulled into the published tarball (it's a devDependency).
    lib: { entry: { index: 'src/index.ts', bin: 'src/bin.ts' }, formats: ['es'] },
    rollupOptions: {
      external: [/^node:/, '@cookiemonsterdev/kafka-core', '@cookiemonsterdev/kafka-config', 'vite'],
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
          // Neither the tarball nor the bundle-budget suite needs a broker (they build and pack
          // the package, or measure what came out of that), so both stay in the unit project
          // alongside everything else `pnpm test` runs.
          include: [
            'src/**/*.test.ts',
            'test/*.test.ts',
            'test/suites/tarball.test.ts',
            'test/suites/bundle-budget.test.ts',
          ],
          environment: 'node',
        },
      },
      {
        test: {
          name: 'integration',
          include: ['test/suites/**/*.test.ts'],
          // Already covered by the unit project above — neither needs a broker, so neither belongs
          // to this project's `globalSetup` — same pattern as core's own vite.config.ts.
          exclude: ['test/suites/tarball.test.ts', 'test/suites/bundle-budget.test.ts'],
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
