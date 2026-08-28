import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { KafkaConfigRequiresAsyncError } from '../errors';
import { resolveKafkaConfig, resolveKafkaConfigAsync, resolveKafkaConfigFrom } from './resolve';

let dir: string | undefined;

afterEach(() => {
  if (dir != null) {
    rmSync(dir, { recursive: true, force: true });
    dir = undefined;
  }
});

function tempDir(): string {
  dir = mkdtempSync(join(tmpdir(), 'kafka-core-config-'));
  return dir;
}

function writeConfig(cwd: string, content: string, filename = 'kafka.config.mjs'): string {
  const path = join(cwd, filename);
  writeFileSync(path, content);
  return path;
}

const ALL_KEYS_CONFIG = `export default {
  client: {
    brokers: ['file:9092'],
    ssl: true,
    sasl: { mechanism: 'plain', username: 'u', password: 'p' },
    clientId: 'file-client',
    connectionTimeout: 111,
    connectionsMaxIdleMs: 222,
    socketConnectionSetupTimeoutMaxMs: 333,
    clientDnsLookup: 'canonicalBootstrap',
    reconnectBackoffMs: 444,
    reconnectBackoffMaxMs: 555,
    authenticationTimeout: 666,
    reauthenticationThreshold: 777,
    requestTimeout: 888,
    enforceRequestTimeout: false,
    metadataRecovery: 'none',
    retry: { retries: 3 },
    socketFactory: () => ({ marker: 'file-socket-factory' }),
    logLevel: 1,
    logCreator: () => () => {},
    metrics: true,
    enableMetricsPush: false,
  },
};`;

describe('resolveKafkaConfig', () => {
  it('resolves all 21 KafkaConfig keys from a file when brokers is omitted', () => {
    const cwd = tempDir();
    const path = writeConfig(cwd, ALL_KEYS_CONFIG);

    const result = resolveKafkaConfig({}, { cwd });

    expect(result.path).toBe(path);
    expect(result.config).toMatchObject({
      brokers: ['file:9092'],
      ssl: true,
      sasl: { mechanism: 'plain', username: 'u', password: 'p' },
      clientId: 'file-client',
      connectionTimeout: 111,
      connectionsMaxIdleMs: 222,
      socketConnectionSetupTimeoutMaxMs: 333,
      clientDnsLookup: 'canonicalBootstrap',
      reconnectBackoffMs: 444,
      reconnectBackoffMaxMs: 555,
      authenticationTimeout: 666,
      reauthenticationThreshold: 777,
      requestTimeout: 888,
      enforceRequestTimeout: false,
      metadataRecovery: 'none',
      retry: { retries: 3 },
      logLevel: 1,
      metrics: true,
      enableMetricsPush: false,
    });
    expect(typeof result.config.socketFactory).toBe('function');
    expect(typeof result.config.logCreator).toBe('function');
  });

  it('lets an explicit value win for one key while the file still supplies the other 20', () => {
    const cwd = tempDir();
    writeConfig(cwd, ALL_KEYS_CONFIG);

    const result = resolveKafkaConfig({ config: true, clientId: 'explicit-client' }, { cwd });

    expect(result.config.clientId).toBe('explicit-client');
    expect(result.config.brokers).toEqual(['file:9092']);
    expect(result.config.requestTimeout).toBe(888);
    expect(result.config.enableMetricsPush).toBe(false);
  });

  it('shallow-merges retry, keeping a file key an explicit partial retry does not override', () => {
    const cwd = tempDir();
    writeConfig(cwd, `export default { client: { brokers: ['b:9092'], retry: { retries: 5, maxRetryTime: 1000 } } };`);

    const result = resolveKafkaConfig({ config: true, retry: { retries: 9 } }, { cwd });

    expect(result.config.retry).toEqual({ retries: 9, maxRetryTime: 1000 });
  });

  it('replaces sasl atomically instead of merging mechanisms', () => {
    const cwd = tempDir();
    writeConfig(
      cwd,
      `export default { client: { brokers: ['b:9092'], sasl: { mechanism: 'plain', username: 'file-user', password: 'file-pass' } } };`,
    );

    const result = resolveKafkaConfig(
      { config: true, sasl: { mechanism: 'scram-sha-256', username: 'explicit-user', password: 'explicit-pass' } },
      { cwd },
    );

    expect(result.config.sasl).toEqual({
      mechanism: 'scram-sha-256',
      username: 'explicit-user',
      password: 'explicit-pass',
    });
  });

  it('does not read a kafka.config.* file when brokers is already given and config is omitted', () => {
    const cwd = tempDir();
    writeConfig(cwd, `export default { client: { clientId: 'should-not-be-used' } };`);

    const result = resolveKafkaConfig({ brokers: ['explicit:9092'] }, { cwd });

    expect(result.path).toBeNull();
    expect(result.fileConfig).toBeNull();
    expect(result.config.brokers).toEqual(['explicit:9092']);
    expect(result.config.clientId).toBeUndefined();
  });

  it('always discovers when config: true, even though brokers is already given', () => {
    const cwd = tempDir();
    writeConfig(cwd, `export default { client: { clientId: 'from-file' } };`);

    const result = resolveKafkaConfig({ brokers: ['explicit:9092'], config: true }, { cwd });

    expect(result.path).not.toBeNull();
    expect(result.config.clientId).toBe('from-file');
  });

  it('never discovers when config: false, even with no brokers anywhere', () => {
    const cwd = tempDir();
    writeConfig(cwd, `export default { client: { brokers: ['should-not-be-used:9092'] } };`);

    expect(() => resolveKafkaConfig({ config: false }, { cwd })).toThrowError(
      expect.objectContaining({ name: 'KafkaConfigError', tag: 'MissingBrokers' }),
    );
  });

  it('loads an explicit string config path, resolved against cwd', () => {
    const cwd = tempDir();
    writeConfig(cwd, `export default { client: { brokers: ['explicit-path:9092'] } };`, 'my-config.mjs');

    const result = resolveKafkaConfig({ config: 'my-config.mjs' }, { cwd });

    expect(result.path).toBe(join(cwd, 'my-config.mjs'));
    expect(result.config.brokers).toEqual(['explicit-path:9092']);
  });

  it('throws ConfigFileNotFound for a missing explicit config path, never falling back silently', () => {
    const cwd = tempDir();

    expect(() => resolveKafkaConfig({ config: 'does-not-exist.mjs' }, { cwd })).toThrowError(
      expect.objectContaining({ name: 'KafkaConfigError', tag: 'ConfigFileNotFound' }),
    );
  });

  it('throws MissingBrokers naming the searched directory when nothing resolves brokers', () => {
    const cwd = tempDir();

    expect(() => resolveKafkaConfig({}, { cwd })).toThrowError(
      expect.objectContaining({
        name: 'KafkaConfigError',
        tag: 'MissingBrokers',
        message: expect.stringContaining(cwd),
      }),
    );
  });

  it('emits a config.loaded diagnostic naming the resolved path', () => {
    const cwd = tempDir();
    const path = writeConfig(cwd, `export default { client: { brokers: ['b:9092'] } };`);
    const onDiagnostic = vi.fn();

    resolveKafkaConfig({}, { cwd, onDiagnostic });

    expect(onDiagnostic).toHaveBeenCalledWith(expect.objectContaining({ code: 'config.loaded', path }));
  });

  it('rejects a config file requiring top-level await, naming Kafka.fromConfig as the remedy', () => {
    const cwd = tempDir();
    writeConfig(cwd, `const brokers = await Promise.resolve(['tla:9092']); export default { client: { brokers } };`);

    expect(() => resolveKafkaConfig({}, { cwd })).toThrowError(KafkaConfigRequiresAsyncError);
  });
});

describe('resolveKafkaConfigAsync', () => {
  it('resolves the same merged config as the sync path for a fixture without top-level await', () => {
    const cwd = tempDir();
    writeConfig(cwd, ALL_KEYS_CONFIG.replace(/socketFactory:.*,\n/, '').replace(/logCreator:.*,\n/, ''));

    const syncResult = resolveKafkaConfig({}, { cwd });
    return resolveKafkaConfigAsync({}, { cwd }).then((asyncResult) => {
      expect(asyncResult.config).toEqual(syncResult.config);
      expect(asyncResult.path).toBe(syncResult.path);
    });
  });

  it('resolves a config file that uses top-level await, which the sync path cannot', async () => {
    const cwd = tempDir();
    writeConfig(cwd, `const brokers = await Promise.resolve(['tla:9092']); export default { client: { brokers } };`);

    const result = await resolveKafkaConfigAsync({}, { cwd });

    expect(result.config.brokers).toEqual(['tla:9092']);
  });

  it('resolves an async factory export', async () => {
    const cwd = tempDir();
    writeConfig(cwd, `export default async () => ({ client: { brokers: ['async-factory:9092'] } });`);

    const result = await resolveKafkaConfigAsync({}, { cwd });

    expect(result.config.brokers).toEqual(['async-factory:9092']);
  });
});

describe('resolveKafkaConfigFrom', () => {
  it('merges an already-loaded file config with overrides — no discovery, no fs reads', () => {
    const config = resolveKafkaConfigFrom(
      { client: { brokers: ['file:9092'], clientId: 'file-client' } },
      { clientId: 'explicit-client' },
    );

    expect(config.brokers).toEqual(['file:9092']);
    expect(config.clientId).toBe('explicit-client');
  });

  it('throws MissingBrokers when neither the file nor the overrides carry brokers', () => {
    expect(() => resolveKafkaConfigFrom({ client: { clientId: 'no-brokers' } })).toThrowError(
      expect.objectContaining({ name: 'KafkaConfigError', tag: 'MissingBrokers' }),
    );
  });
});
