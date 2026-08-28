import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { createAdmin } from './admin/index';
import type * as AdminModule from './admin/index';
import { Kafka } from './client';
import { resolveKafkaConfig } from './config/resolve';
import { createConsumer } from './consumer/index';
import type * as ConsumerModule from './consumer/index';
import { logLevel } from './index';
import { createProducer } from './producer/index';
import type * as ProducerModule from './producer/index';
import { createShareConsumer } from './share-consumer/index';
import type * as ShareConsumerModule from './share-consumer/index';

vi.mock('./producer/index', async (importOriginal) => {
  const actual = await importOriginal<typeof ProducerModule>();
  return { ...actual, createProducer: vi.fn(actual.createProducer) };
});
vi.mock('./consumer/index', async (importOriginal) => {
  const actual = await importOriginal<typeof ConsumerModule>();
  return { ...actual, createConsumer: vi.fn(actual.createConsumer) };
});
vi.mock('./share-consumer/index', async (importOriginal) => {
  const actual = await importOriginal<typeof ShareConsumerModule>();
  return { ...actual, createShareConsumer: vi.fn(actual.createShareConsumer) };
});
vi.mock('./admin/index', async (importOriginal) => {
  const actual = await importOriginal<typeof AdminModule>();
  return { ...actual, createAdmin: vi.fn(actual.createAdmin) };
});

let dir: string | undefined;

afterEach(() => {
  vi.clearAllMocks();
  if (dir != null) {
    rmSync(dir, { recursive: true, force: true });
    dir = undefined;
  }
});

function tempDir(): string {
  dir = mkdtempSync(join(tmpdir(), 'kafka-core-client-config-'));
  return dir;
}

function writeConfig(cwd: string, content: string, filename = 'kafka.config.mjs'): void {
  writeFileSync(join(cwd, filename), content);
}

function quietOptions() {
  return { logLevel: logLevel.NOTHING, logCreator: () => () => {} };
}

/** Resolves a config against an explicit temp `cwd` and builds a `Kafka` from it, bypassing the constructor's own `process.cwd()`-based default so tests stay hermetic (no `process.chdir`). */
function kafkaFromCwd(explicit: ConstructorParameters<typeof Kafka>[0] = {}, cwd: string): Kafka {
  const merged = { ...quietOptions(), ...explicit };
  return new Kafka(merged, resolveKafkaConfig(merged, { cwd }));
}

describe('Kafka — constructor config resolution', () => {
  it('resolves brokers and other options from a config file when brokers is omitted', () => {
    const cwd = tempDir();
    writeConfig(cwd, `export default { client: { brokers: ['file:9092'], clientId: 'from-file' } };`);

    const kafka = kafkaFromCwd({}, cwd);

    expect(kafka.configSource().path).toBe(join(cwd, 'kafka.config.mjs'));
    expect(kafka.configSource().keys.brokers).toBe('file');
    expect(kafka.configSource().keys.clientId).toBe('file');
  });

  it('never touches the filesystem when brokers is explicit and config is omitted', () => {
    const kafka = new Kafka({ brokers: ['explicit:9092'], ...quietOptions() });

    expect(kafka.configSource().path).toBeNull();
    expect(kafka.configSource().keys.brokers).toBe('explicit');
  });

  it('throws KafkaConfigError tagged MissingBrokers when nothing resolves brokers', () => {
    expect(() => new Kafka({ ...quietOptions(), config: false })).toThrowError(
      expect.objectContaining({ name: 'KafkaConfigError', tag: 'MissingBrokers' }),
    );
  });
});

describe('Kafka — factory-level file defaults', () => {
  it('producer() picks up a file default it does not pass explicitly', () => {
    const cwd = tempDir();
    writeConfig(cwd, `export default { client: { brokers: ['b:9092'] }, producer: { lingerMs: 42 } };`);
    const kafka = kafkaFromCwd({}, cwd);

    kafka.producer();

    expect(createProducer).toHaveBeenCalledWith(expect.objectContaining({ lingerMs: 42 }));
  });

  it('an explicit producer() argument beats the file default', () => {
    const cwd = tempDir();
    writeConfig(cwd, `export default { client: { brokers: ['b:9092'] }, producer: { lingerMs: 42 } };`);
    const kafka = kafkaFromCwd({}, cwd);

    kafka.producer({ lingerMs: 7 });

    expect(createProducer).toHaveBeenCalledWith(expect.objectContaining({ lingerMs: 7 }));
  });

  it('the file does not override a producer() default it omits', () => {
    const cwd = tempDir();
    writeConfig(cwd, `export default { client: { brokers: ['b:9092'] }, producer: { lingerMs: 42 } };`);
    const kafka = kafkaFromCwd({}, cwd);

    kafka.producer();

    // batchSize has no file entry, so createProducer receives whatever producer()'s own
    // destructuring produced for it — never forced by the (nonexistent) file value.
    const call = vi.mocked(createProducer).mock.calls[0]?.[0];
    expect(call?.batchSize).toBeUndefined();
  });

  it('consumer({groupId}) still works when the file omits groupId', () => {
    const cwd = tempDir();
    writeConfig(cwd, `export default { client: { brokers: ['b:9092'] }, consumer: { sessionTimeout: 12345 } };`);
    const kafka = kafkaFromCwd({}, cwd);

    kafka.consumer({ groupId: 'my-group' });

    expect(createConsumer).toHaveBeenCalledWith(
      expect.objectContaining({ groupId: 'my-group', sessionTimeout: 12345 }),
    );
  });

  it('shareConsumer() merges the file section under the required groupId argument', () => {
    const cwd = tempDir();
    writeConfig(cwd, `export default { client: { brokers: ['b:9092'] }, shareConsumer: { maxRecords: 10 } };`);
    const kafka = kafkaFromCwd({}, cwd);

    kafka.shareConsumer({ groupId: 'share-group' });

    expect(createShareConsumer).toHaveBeenCalledWith(
      expect.objectContaining({ groupId: 'share-group', maxRecords: 10 }),
    );
  });

  it('admin() merges the file section under whatever is passed explicitly', () => {
    const cwd = tempDir();
    writeConfig(cwd, `export default { client: { brokers: ['b:9092'] }, admin: { retry: { retries: 9 } } };`);
    const kafka = kafkaFromCwd({}, cwd);

    kafka.admin();

    expect(createAdmin).toHaveBeenCalledWith(expect.objectContaining({ retry: { retries: 9 } }));
  });

  it('a client constructed with config: false is unaffected by any nearby config file', () => {
    const kafka = new Kafka({ ...quietOptions(), config: false, brokers: ['explicit:9092'] });

    kafka.producer();

    const call = vi.mocked(createProducer).mock.calls[0]?.[0];
    expect(call?.lingerMs).toBeUndefined();
  });
});

describe('Kafka.fromConfig / Kafka.from', () => {
  it('fromConfig() resolves a config file that uses top-level await, which the constructor cannot', async () => {
    const cwd = tempDir();
    writeConfig(cwd, `const brokers = await Promise.resolve(['tla:9092']); export default { client: { brokers } };`);

    const kafka = await Kafka.fromConfig({ ...quietOptions() }, { cwd });

    expect(kafka.configSource().keys.brokers).toBe('file');
    expect(kafka.configSource().path).toBe(join(cwd, 'kafka.config.mjs'));
  });

  it('fromConfig() and the constructor resolve the same merged config for a non-TLA fixture', async () => {
    const cwd = tempDir();
    writeConfig(cwd, `export default { client: { brokers: ['same:9092'], requestTimeout: 555 } };`);

    const viaConstructor = kafkaFromCwd({}, cwd);
    const viaFromConfig = await Kafka.fromConfig({ ...quietOptions() }, { cwd });

    expect(viaFromConfig.configSource().keys).toEqual(viaConstructor.configSource().keys);
  });

  it('from(fileConfig, overrides) is synchronous and applies overrides over the file', () => {
    const kafka = Kafka.from(
      { client: { brokers: ['file:9092'], clientId: 'file-client' } },
      { ...quietOptions(), clientId: 'explicit-client' },
    );

    expect(kafka.configSource().path).toBeNull();
    expect(kafka.configSource().keys.clientId).toBe('explicit');
    expect(kafka.configSource().keys.brokers).toBe('file');
  });

  it('from() throws MissingBrokers identically to the constructor when nothing resolves brokers', () => {
    expect(() => Kafka.from({ client: {} }, { ...quietOptions() })).toThrowError(
      expect.objectContaining({ name: 'KafkaConfigError', tag: 'MissingBrokers' }),
    );
  });
});

describe('Kafka.configSource', () => {
  it('reports a null path and every key default for a client with no config file', () => {
    const kafka = new Kafka({ brokers: ['explicit:9092'], ...quietOptions() });

    const source = kafka.configSource();
    expect(source.path).toBeNull();
    expect(source.keys.retry).toBe('default');
    expect(source.keys.connectionTimeout).toBe('default');
  });

  it('never carries a secret value, only provenance tags', () => {
    const kafka = new Kafka({
      brokers: ['explicit:9092'],
      sasl: { mechanism: 'plain', username: 'u', password: 'super-secret' },
      ...quietOptions(),
    });

    const serialized = JSON.stringify(kafka.configSource());
    expect(serialized).not.toContain('super-secret');
  });
});
