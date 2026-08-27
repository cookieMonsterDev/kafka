import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { afterEach, describe, expect, it } from 'vitest';
import type { ConfigDiagnostic } from './diagnostics';
import type { KafkaConfigError } from './errors';
import { loadKafkaConfig, type LoadKafkaConfigResult } from './load';
import type { KafkaFileConfig } from './types';

const FIXTURES = join(dirname(fileURLToPath(import.meta.url)), '../../test/fixtures/config/load');

function assertNoGetters(value: object): void {
  for (const key of Object.getOwnPropertyNames(value)) {
    const descriptor = Object.getOwnPropertyDescriptor(value, key);
    expect(descriptor).toBeDefined();
    expect('get' in (descriptor ?? {})).toBe(false);
  }
}

/** Narrows an `{ok:false}` result and returns its error, asserting the discriminant unconditionally. */
function expectFailure(result: LoadKafkaConfigResult): KafkaConfigError {
  expect(result.ok).toBe(false);
  return (result as { ok: false; error: KafkaConfigError }).error;
}

/** Narrows an `{ok:true}` result and returns its config, asserting the discriminant unconditionally. */
function expectSuccess(result: LoadKafkaConfigResult): KafkaFileConfig {
  expect(result.ok).toBe(true);
  return (result as { ok: true; config: KafkaFileConfig }).config;
}

describe('loadKafkaConfig', () => {
  const tempDirs: string[] = [];

  function makeEmptyTempDir(): string {
    const dir = mkdtempSync(join(tmpdir(), 'kafka-load-kafka-config-'));
    tempDirs.push(dir);
    return dir;
  }

  afterEach(() => {
    for (const dir of tempDirs.splice(0)) {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  it('returns {ok:true, resolvedPath:null, config:{}} when nothing is found', () => {
    const cwd = makeEmptyTempDir();

    const result = loadKafkaConfig({ cwd, searchParents: false });

    expect(result).toEqual({ ok: true, resolvedPath: null, config: {} });
    const config = expectSuccess(result);
    expect(Object.isFrozen(config)).toBe(true);
    assertNoGetters(config);
  });

  it('loads the found fixture', () => {
    const result = loadKafkaConfig({ cwd: join(FIXTURES, 'found'), searchParents: false });

    expect(result).toEqual({
      ok: true,
      resolvedPath: join(FIXTURES, 'found', 'kafka.config.ts'),
      config: { client: { brokers: ['found:9092'] } },
    });
    const config = expectSuccess(result);
    expect(Object.isFrozen(config)).toBe(true);
    assertNoGetters(config);
  });

  it('errors with ConfigFileNotFound for a missing explicit path, with no fallback search', () => {
    // cwd has a perfectly loadable config; if a fallback search happened, this would succeed.
    const result = loadKafkaConfig({ cwd: join(FIXTURES, 'found'), path: 'does-not-exist.config.ts' });

    const error = expectFailure(result);
    expect(error.tag).toBe('ConfigFileNotFound');
    expect(error.path).toContain('does-not-exist.config.ts');
  });

  it('wraps a module that throws during evaluation as ConfigLoadError with a cause', () => {
    const path = join(FIXTURES, 'throwing', 'kafka.config.ts');

    const result = loadKafkaConfig({ cwd: FIXTURES, path });

    const error = expectFailure(result);
    expect(error.tag).toBe('ConfigLoadError');
    expect(error.cause).toBeDefined();
  });

  it('tags a string default export as ConfigFileInvalid', () => {
    const path = join(FIXTURES, 'string-export', 'kafka.config.ts');

    const result = loadKafkaConfig({ cwd: FIXTURES, path });

    const error = expectFailure(result);
    expect(error.tag).toBe('ConfigFileInvalid');
  });

  it('tags an unsupported extension (.yaml) as UnsupportedExtension', () => {
    const path = join(FIXTURES, 'kafka.config.yaml');

    const result = loadKafkaConfig({ cwd: FIXTURES, path });

    const error = expectFailure(result);
    expect(error.tag).toBe('UnsupportedExtension');
    expect(error.message).toContain('.yaml');
  });

  it('emits a config.loaded diagnostic when a file is found', () => {
    const diagnostics: ConfigDiagnostic[] = [];

    loadKafkaConfig({
      cwd: join(FIXTURES, 'found'),
      searchParents: false,
      onDiagnostic: (diagnostic) => diagnostics.push(diagnostic),
    });

    const expectedPath = join(FIXTURES, 'found', 'kafka.config.ts');
    expect(diagnostics.some((d) => d.code === 'config.loaded' && d.path === expectedPath)).toBe(true);
  });

  it('emits a config.loaded diagnostic even when nothing is found', () => {
    const cwd = makeEmptyTempDir();
    const diagnostics: ConfigDiagnostic[] = [];

    loadKafkaConfig({ cwd, searchParents: false, onDiagnostic: (diagnostic) => diagnostics.push(diagnostic) });

    expect(diagnostics.some((d) => d.code === 'config.loaded')).toBe(true);
  });

  it('honours KAFKA_CONFIG when no explicit path option is given', () => {
    const path = join(FIXTURES, 'found', 'kafka.config.ts');

    const result = loadKafkaConfig({ cwd: FIXTURES, env: { KAFKA_CONFIG: path } });

    expect(result).toEqual({ ok: true, resolvedPath: path, config: { client: { brokers: ['found:9092'] } } });
  });

  it('prefers an explicit path option over KAFKA_CONFIG', () => {
    const explicit = join(FIXTURES, 'found', 'kafka.config.ts');
    const viaEnv = join(FIXTURES, 'string-export', 'kafka.config.ts');

    const result = loadKafkaConfig({ cwd: FIXTURES, path: explicit, env: { KAFKA_CONFIG: viaEnv } });

    expect(result).toEqual({ ok: true, resolvedPath: explicit, config: { client: { brokers: ['found:9092'] } } });
  });

  it('never throws — every failure mode comes back as {ok:false}', () => {
    const scenarios = [
      { cwd: join(FIXTURES, 'found'), path: 'missing.config.ts' },
      { cwd: FIXTURES, path: join(FIXTURES, 'throwing', 'kafka.config.ts') },
      { cwd: FIXTURES, path: join(FIXTURES, 'string-export', 'kafka.config.ts') },
      { cwd: FIXTURES, path: join(FIXTURES, 'kafka.config.yaml') },
    ];

    for (const scenario of scenarios) {
      expect(() => loadKafkaConfig(scenario)).not.toThrow();
      expect(loadKafkaConfig(scenario).ok).toBe(false);
    }
  });
});
