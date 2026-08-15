import { defineConfig } from 'vitest/config'

export default defineConfig({
  build: {
    target: 'node24',
    sourcemap: true,
    minify: false,
    lib: { entry: 'src/index.ts', formats: ['es'] },
    rollupOptions: {
      // D5: zero runtime deps, so builtins are the entire external surface
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
    projects: [
      {
        test: {
          name: 'unit',
          include: ['src/**/*.test.ts'],
          environment: 'node',
        },
      },
      {
        test: {
          name: 'integration',
          include: ['test/suites/**/*.test.ts'],
          environment: 'node',
          globalSetup: ['./test/helpers/globalSetup.ts'],
          testTimeout: 30_000,
          hookTimeout: 60_000,
          fileParallelism: true,
          retry: Number(process.env.TEST_RETRIES ?? 0),
        },
      },
    ],
    coverage: {
      provider: 'v8',
      reporter: ['text', 'lcov'],
      thresholds: {
        // Phase 0 gate: no source ported yet, nothing to ratchet against.
      },
    },
  },
})
