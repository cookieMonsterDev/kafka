import { defineConfig } from 'vitest/config';

export default defineConfig({
  build: {
    target: 'node24',
    sourcemap: true,
    minify: false,
    // Bundled to a single file, unlike core/kafka-config's preserveModules build: the CLI's own
    // code should cost one cheap parse on every invocation, including --help/--version, and core
    // (kept external below) is meant to be await-imported only inside commands that connect.
    lib: { entry: { index: 'src/index.ts', bin: 'src/bin.ts' }, formats: ['es'] },
    rollupOptions: {
      external: [/^node:/, '@cookiemonsterdev/kafka-core', '@cookiemonsterdev/kafka-config'],
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
          include: [
            'test/suites/topic-lifecycle.test.ts',
            'test/suites/config-lifecycle.test.ts',
            'test/suites/group-lifecycle.test.ts',
            'test/suites/acl-lifecycle.test.ts',
            'test/suites/cluster-lifecycle.test.ts',
          ],
          environment: 'node',
          globalSetup: ['./test/helpers/global-setup.ts'],
          testTimeout: 30_000,
          hookTimeout: 60_000,
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
