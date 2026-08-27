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
  });
});
