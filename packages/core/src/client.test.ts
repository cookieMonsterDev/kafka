import { describe, expect, it, vi } from 'vitest';
import { Kafka } from './client';
import { KafkaNonRetriableError } from './errors';
import { logLevel, Partitioners, type KafkaConfig } from './index';

function createClient(overrides: Partial<KafkaConfig> = {}): Kafka {
  return new Kafka({
    brokers: ['localhost:9092'],
    logLevel: logLevel.NOTHING,
    logCreator: () => () => {},
    ...overrides,
  });
}

describe('Kafka', () => {
  it('exposes a logger', () => {
    const kafka = createClient();
    expect(typeof kafka.logger().info).toBe('function');
  });

  it('creates a producer with the public send/connect surface', () => {
    const producer = createClient().producer();
    expect(typeof producer.send).toBe('function');
    expect(typeof producer.sendBatch).toBe('function');
    expect(typeof producer.flush).toBe('function');
    expect(typeof producer.connect).toBe('function');
    expect(typeof producer.disconnect).toBe('function');
    expect(typeof producer[Symbol.asyncDispose]).toBe('function');
    expect(producer.isIdempotent()).toBe(false);
  });

  it('creates a consumer that requires a groupId', () => {
    expect(() => createClient().consumer({ groupId: '' })).toThrow(KafkaNonRetriableError);

    const consumer = createClient().consumer({ groupId: 'test-group' });
    expect(typeof consumer.subscribe).toBe('function');
    expect(typeof consumer.run).toBe('function');
    expect(typeof consumer.stream).toBe('function');
    expect(typeof consumer[Symbol.asyncDispose]).toBe('function');
  });

  it('creates an admin client', () => {
    const admin = createClient().admin();
    expect(typeof admin.listTopics).toBe('function');
    expect(typeof admin.createTopics).toBe('function');
    expect(typeof admin[Symbol.asyncDispose]).toBe('function');
  });

  it('disconnects without connecting, so await using is safe on a fresh client', async () => {
    const producer = createClient().producer();
    await expect(producer[Symbol.asyncDispose]()).resolves.toBeUndefined();
  });

  it('rejects connect when the signal is already aborted', async () => {
    const producer = createClient().producer();
    await expect(producer.connect({ signal: AbortSignal.abort('nope') })).rejects.toMatchObject({
      message: 'Aborted',
      cause: 'nope',
    });
  });

  it('warns once per client when the default partitioner is used', () => {
    const warnings: string[] = [];
    const kafka = createClient({
      logLevel: logLevel.WARN,
      logCreator: () => (entry) => {
        if (entry.label === 'WARN') warnings.push(entry.log.message);
      },
    });

    kafka.producer();
    kafka.producer();
    expect(warnings).toHaveLength(1);
    expect(warnings[0]).toMatch(/default partitioner/i);
  });

  it('does not warn when a partitioner is provided', () => {
    const warnings: string[] = [];
    const kafka = createClient({
      logLevel: logLevel.WARN,
      logCreator: () => (entry) => {
        if (entry.label === 'WARN') warnings.push(entry.log.message);
      },
    });

    kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner });
    expect(warnings).toHaveLength(0);
  });

  it('silences the default-partitioner warning when the env var is set', () => {
    vi.stubEnv('KAFKA_NO_PARTITIONER_WARNING', '1');
    const warnings: string[] = [];
    const kafka = createClient({
      logLevel: logLevel.WARN,
      logCreator: () => (entry) => {
        if (entry.label === 'WARN') warnings.push(entry.log.message);
      },
    });

    kafka.producer();
    expect(warnings).toHaveLength(0);
    vi.unstubAllEnvs();
  });

  it('accepts a brokers function', () => {
    const kafka = createClient({ brokers: () => ['localhost:9092'] });
    expect(typeof kafka.producer().send).toBe('function');
  });
});
