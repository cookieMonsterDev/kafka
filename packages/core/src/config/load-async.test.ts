import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';
import { loadConfigFileAsync } from './load-async';
import { loadConfigFileSync } from './load-sync';

const FIXTURES = join(dirname(fileURLToPath(import.meta.url)), '../../test/fixtures/config/load-sync');

describe('loadConfigFileAsync', () => {
  it.each([
    ['kafka.config.ts', 'ts:9092'],
    ['kafka.config.mts', 'mts:9092'],
    ['kafka.config.cts', 'cts:9092'],
    ['kafka.config.js', 'js:9092'],
    ['kafka.config.mjs', 'mjs:9092'],
    ['kafka.config.cjs', 'cjs:9092'],
    ['kafka.config.json', 'json:9092'],
  ])('loads %s', async (filename, broker) => {
    const config = await loadConfigFileAsync(join(FIXTURES, 'ladder', filename));

    expect(config).toEqual({ client: { brokers: [broker] } });
  });

  it('loads a config that requires top-level await, which the sync loader rejects', async () => {
    const path = join(FIXTURES, 'tla', 'kafka.config.ts');

    expect(() => loadConfigFileSync(path)).toThrow();
    await expect(loadConfigFileAsync(path)).resolves.toEqual({ client: { brokers: ['tla:9092'] } });
  });

  it('awaits a sync factory export', async () => {
    const config = await loadConfigFileAsync(join(FIXTURES, 'factory', 'kafka.config.ts'));

    expect(config).toEqual({ client: { brokers: ['call-1:9092'] } });
  });

  it('awaits an async factory export', async () => {
    const config = await loadConfigFileAsync(join(FIXTURES, 'async-factory', 'kafka.config.ts'));

    expect(config).toEqual({ client: { brokers: ['async-factory:9092'] } });
  });

  // A path containing a space and "#" is covered by load-sync.test.ts (createRequire) and verified
  // directly against plain `import()` — Vite/vitest's own dynamic-import transform mishandles a
  // literal "#" in a file URL even when percent-encoded by pathToFileURL, which is a test-runner
  // limitation, not a bug in this loader.

  // Deliberately excludes the `transform-hooks/enum` and `transform-hooks/extensionless`
  // fixtures: the D8 rescue is `registerHooks`-based (CommonJS `require()` only) and has no
  // effect on `import()`, so the two loaders are known and documented to diverge on those two
  // constructs specifically (see the JSDoc on `loadConfigFileAsync`). This block only asserts
  // parity for everything else.
  describe('anti-drift: agrees with the sync loader for every non-TLA, non-rescue fixture', () => {
    it.each([
      ['ladder/kafka.config.ts'],
      ['ladder/kafka.config.mts'],
      ['ladder/kafka.config.cts'],
      ['ladder/kafka.config.js'],
      ['ladder/kafka.config.mjs'],
      ['ladder/kafka.config.cjs'],
      ['ladder/kafka.config.json'],
      ['commonjs-package/kafka.config.ts'],
    ])('%s', async (relativePath) => {
      const path = join(FIXTURES, relativePath);

      const syncResult = loadConfigFileSync(path);
      const asyncResult = await loadConfigFileAsync(path);

      expect(asyncResult).toEqual(syncResult);
    });
  });
});
