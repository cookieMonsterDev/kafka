import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';
import { KafkaConfigRequiresAsyncError } from './errors';
import { loadConfigFileSync } from './load-sync';

const FIXTURES = join(dirname(fileURLToPath(import.meta.url)), '../../test/fixtures/config/load-sync');

describe('loadConfigFileSync', () => {
  it.each([
    ['kafka.config.ts', 'ts:9092'],
    ['kafka.config.mts', 'mts:9092'],
    ['kafka.config.cts', 'cts:9092'],
    ['kafka.config.js', 'js:9092'],
    ['kafka.config.mjs', 'mjs:9092'],
    ['kafka.config.cjs', 'cjs:9092'],
    ['kafka.config.json', 'json:9092'],
  ])('loads %s', (filename, broker) => {
    const config = loadConfigFileSync(join(FIXTURES, 'ladder', filename));

    expect(config).toEqual({ client: { brokers: [broker] } });
  });

  it('loads a .ts file under a package.json with "type": "commonjs"', () => {
    const config = loadConfigFileSync(join(FIXTURES, 'commonjs-package', 'kafka.config.ts'));

    expect(config).toEqual({ client: { brokers: ['commonjs-pkg:9092'] } });
  });

  it('rescues "export default" in a .ts file under an untyped package.json (Node auto-detects ESM natively)', () => {
    const diagnostics: string[] = [];

    const config = loadConfigFileSync(join(FIXTURES, 'esm-export-under-untyped', 'kafka.config.ts'), {
      onDiagnostic: (d) => diagnostics.push(d.code),
    });

    expect(config).toEqual({ client: { brokers: ['esm-export-untyped:9092'] } });
    expect(diagnostics).not.toContain('config.transform-fallback');
  });

  it('rescues "export default" in a .ts file under "type": "commonjs" (D8 fallback)', () => {
    const diagnostics: { code: string; detail?: unknown }[] = [];
    const path = join(FIXTURES, 'esm-export-under-commonjs-typed', 'kafka.config.ts');

    const config = loadConfigFileSync(path, {
      onDiagnostic: (d) => diagnostics.push({ code: d.code, detail: d.detail }),
    });

    expect(config).toEqual({ client: { brokers: ['esm-export-cjs-typed:9092'] } });
    const fallback = diagnostics.find((d) => d.code === 'config.transform-fallback');
    expect(fallback).toBeDefined();
    expect(fallback?.detail).toContain('ES module syntax');
  });

  it('loads a path containing a space and a "#" via pathToFileURL, never string concatenation', () => {
    const config = loadConfigFileSync(join(FIXTURES, 'weird path #1', 'kafka.config.ts'));

    expect(config).toEqual({ client: { brokers: ['weird-path:9092'] } });
  });

  it('throws KafkaConfigRequiresAsyncError, naming the file and Kafka.fromConfig(), for top-level await', () => {
    const path = join(FIXTURES, 'tla', 'kafka.config.ts');

    let thrown: unknown;
    try {
      loadConfigFileSync(path);
    } catch (error) {
      thrown = error;
    }

    expect(thrown).toBeInstanceOf(KafkaConfigRequiresAsyncError);
    const error = thrown as KafkaConfigRequiresAsyncError;
    expect(error.path).toBe(path);
    expect(error.message).toContain(path);
    expect(error.message).toContain('Kafka.fromConfig()');
  });

  it('wraps a JSON parse failure in a ConfigLoadError naming the file', () => {
    const path = join(FIXTURES, 'invalid-json', 'kafka.config.json');

    expect(() => loadConfigFileSync(path)).toThrow(
      expect.objectContaining({ tag: 'ConfigLoadError', path, cause: expect.anything() }),
    );
  });

  describe('memoisation', () => {
    it('does not re-read a JSON config file on a second call, even after it changes on disk', () => {
      const dir = mkdtempSync(join(tmpdir(), 'kafka-config-load-sync-'));
      const path = join(dir, 'kafka.config.json');
      writeFileSync(path, JSON.stringify({ client: { brokers: ['first:9092'] } }));

      try {
        const first = loadConfigFileSync(path);
        writeFileSync(path, JSON.stringify({ client: { brokers: ['second:9092'] } }));
        const second = loadConfigFileSync(path);

        expect(first).toEqual({ client: { brokers: ['first:9092'] } });
        expect(second).toBe(first);
      } finally {
        rmSync(dir, { recursive: true, force: true });
      }
    });

    it('invokes a factory export only once across repeated calls, proving the result is cached', () => {
      const path = join(FIXTURES, 'factory', 'kafka.config.ts');

      const first = loadConfigFileSync(path);
      const second = loadConfigFileSync(path);

      expect(first).toEqual({ client: { brokers: ['call-1:9092'] } });
      expect(second).toBe(first);
    });

    it('gives allowTransformFallback: true and false independent cache entries for the same path', () => {
      // Uses a call-counting factory with no rescuable construct at all (loads via the plain
      // native path either way), so this isolates the cache key itself from whether the
      // transform-hook fallback happens to already be installed elsewhere in the process (see the
      // "known limitation" test below for that separate concern).
      const path = join(FIXTURES, 'cache-key-factory', 'kafka.config.ts');

      const lenient1 = loadConfigFileSync(path);
      const lenient2 = loadConfigFileSync(path);
      const strict1 = loadConfigFileSync(path, { allowTransformFallback: false });
      const strict2 = loadConfigFileSync(path, { allowTransformFallback: false });

      expect(lenient1).toEqual({ client: { brokers: ['cache-key-call-1:9092'] } });
      expect(lenient2).toBe(lenient1);
      // A single cache keyed only by `path` would return `lenient1` here instead of invoking the
      // factory again — proving the two options get independent cache entries.
      expect(strict1).toEqual({ client: { brokers: ['cache-key-call-2:9092'] } });
      expect(strict1).not.toBe(lenient1);
      expect(strict2).toBe(strict1);
    });

    it('known limitation: once the transform fallback is installed anywhere in the process, a later strict call for a rescuable file no longer throws', () => {
      // `module.registerHooks` (D8) has no `deregister` on this Node version (see
      // transform-hooks.ts) — once any earlier lenient load anywhere in the process installs the
      // fallback hooks, `require()` itself silently rescues a rescuable file on every later
      // attempt, including one made with `allowTransformFallback: false`. The cache-key fix above
      // stops a *stale cached value* from masking this, but it cannot make `require()` fail again
      // once Node's global hook state has already changed — that would need `registerHooks`
      // deregistration, which does not exist. Pinned here so this residual behavior is an explicit,
      // tested contract instead of a silent surprise. A process that needs the CI guarantee to be
      // airtight must set `allowTransformFallback: false` for every call from process start, never
      // mixing it with a lenient call for a potentially-rescuable file in the same process.
      const path = join(FIXTURES, 'cache-key-enum', 'kafka.config.ts');

      const lenient = loadConfigFileSync(path);
      expect(lenient).toEqual({ client: { brokers: ['cache-key-enum:info'] } });

      // Would ideally throw; documented here as a known limitation, not asserted as a bug.
      const strict = loadConfigFileSync(path, { allowTransformFallback: false });
      expect(strict).toEqual(lenient);
    });
  });
});
