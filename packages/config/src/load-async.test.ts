import { execFileSync } from 'node:child_process';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';
import { loadConfigFileAsync } from './load-async';
import { loadConfigFileSync } from './load-sync';
import { isBun } from './runtime';

const HERE = dirname(fileURLToPath(import.meta.url));
const FIXTURES = join(HERE, '../test/fixtures/load-sync');
const ALL_FIXTURES = join(HERE, '../test/fixtures');
const DRIVER = join(HERE, '../test/helpers/run-load-async.mjs');

interface DriverResult {
  ok: boolean;
  config?: unknown;
  name?: string;
  tag?: string;
  message?: string;
}

/**
 * Runs the async loader in a fresh process of the current runtime (`node` or `bun`): in-process,
 * vitest's own `import()` transform strips enums and resolves extensionless imports, hiding what
 * the runtime itself does.
 */
function runLoadAsync(configPath: string): DriverResult {
  const output = execFileSync(process.execPath, [DRIVER, configPath], {
    encoding: 'utf8',
    stdio: ['ignore', 'pipe', 'ignore'],
  });
  return JSON.parse(output) as DriverResult;
}

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

  it('loads a config that requires top-level await (which the sync loader rejects on Node)', async () => {
    const path = join(FIXTURES, 'tla', 'kafka.config.ts');

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

  // Deliberately excludes the `transform-hooks/*` fixtures: the D8 rescue is `registerHooks`-based
  // (CommonJS `require()` only) and has no effect on `import()`, so the two loaders are known and
  // documented to diverge on those constructs specifically (see the JSDoc on `loadConfigFileAsync`). This block only asserts
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

  // No transform-hook rescue on the async path (it can't hook `import()`): on Node these fail with
  // an error naming the construct and the fix; Bun's `import()` loads them natively.
  describe('constructs the sync loader would rescue (subprocess)', () => {
    const cases = [
      {
        label: 'an extensionless relative import',
        rel: 'transform-hooks/extensionless/kafka.config.ts',
        bunConfig: { client: { brokers: ['extensionless:9092'] } },
        needles: ['missing its file extension', 'add the ".ts"'],
      },
      {
        label: 'ES module syntax under "type": "commonjs"',
        rel: 'load-sync/esm-export-under-commonjs-typed/kafka.config.ts',
        bunConfig: { client: { brokers: ['esm-export-cjs-typed:9092'] } },
        needles: ['resolves to CommonJS', 'rename the file to ".mts"'],
      },
      {
        label: 'a TypeScript enum',
        rel: 'transform-hooks/enum/kafka.config.ts',
        bunConfig: { client: { brokers: ['enum:info'] } },
        needles: ['enum', 'frozen object'],
      },
    ];

    it.skipIf(isBun()).each(cases)('on Node, fails $label with an error naming the fix', ({ rel, needles }) => {
      const path = join(ALL_FIXTURES, rel);

      const result = runLoadAsync(path);

      expect(result).toMatchObject({ ok: false, name: 'KafkaConfigError', tag: 'ConfigLoadError' });
      expect(result.message).toContain(path);
      for (const needle of needles) {
        expect(result.message).toContain(needle);
      }
    });

    it.runIf(isBun()).each(cases)('on Bun, loads $label natively', ({ rel, bunConfig }) => {
      expect(runLoadAsync(join(ALL_FIXTURES, rel))).toEqual({ ok: true, config: bunConfig });
    });
  });
});
