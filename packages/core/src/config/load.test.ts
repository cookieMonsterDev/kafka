import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';
import { KafkaConfigError, KafkaConfigRequiresAsyncError } from '../errors';
import { loadKafkaConfig, loadKafkaConfigAsync } from './load';

let dir: string | undefined;

afterEach(() => {
  if (dir != null) {
    rmSync(dir, { recursive: true, force: true });
    dir = undefined;
  }
});

function tempFile(content: string, filename: string): string {
  dir = mkdtempSync(join(tmpdir(), 'kafka-core-config-load-'));
  const path = join(dir, filename);
  writeFileSync(path, content);
  return path;
}

describe('loadKafkaConfig', () => {
  it('loads and validates a JSON config', () => {
    const path = tempFile('{ "client": { "brokers": ["a:9092"] } }', 'kafka.config.json');

    expect(loadKafkaConfig(path)).toEqual({ client: { brokers: ['a:9092'] } });
  });

  it("wraps a generic loader failure into this client's own KafkaConfigError, never leaking the foreign type", () => {
    const path = tempFile('{ not valid json', 'kafka.config.json');

    let caught: unknown;
    try {
      loadKafkaConfig(path);
    } catch (error) {
      caught = error;
    }

    expect(caught).toBeInstanceOf(KafkaConfigError);
    expect((caught as KafkaConfigError).tag).toBe('ConfigLoadError');
    expect((caught as KafkaConfigError).path).toBe(path);
  });

  it('rejects a known section that is not an object', () => {
    const path = tempFile('{ "admin": [] }', 'kafka.config.json');

    expect(() => loadKafkaConfig(path)).toThrow(KafkaConfigError);
  });

  it("wraps a top-level-await config into this client's own KafkaConfigRequiresAsyncError", () => {
    const path = tempFile(
      "const brokers = await Promise.resolve(['tla:9092']); export default { client: { brokers } };",
      'kafka.config.mjs',
    );

    let caught: unknown;
    try {
      loadKafkaConfig(path);
    } catch (error) {
      caught = error;
    }

    expect(caught).toBeInstanceOf(KafkaConfigRequiresAsyncError);
    expect((caught as KafkaConfigRequiresAsyncError).path).toBe(path);
  });
});

describe('loadKafkaConfigAsync', () => {
  it('loads and validates a JSON config', async () => {
    const path = tempFile('{ "client": { "brokers": ["a:9092"] } }', 'kafka.config.json');

    await expect(loadKafkaConfigAsync(path)).resolves.toEqual({ client: { brokers: ['a:9092'] } });
  });

  it('resolves a config file that uses top-level await', async () => {
    const path = tempFile(
      "const brokers = await Promise.resolve(['tla:9092']); export default { client: { brokers } };",
      'kafka.config.mjs',
    );

    await expect(loadKafkaConfigAsync(path)).resolves.toEqual({ client: { brokers: ['tla:9092'] } });
  });

  it("wraps a generic loader failure into this client's own KafkaConfigError", async () => {
    const path = tempFile('{ not valid json', 'kafka.config.json');

    await expect(loadKafkaConfigAsync(path)).rejects.toBeInstanceOf(KafkaConfigError);
  });
});
