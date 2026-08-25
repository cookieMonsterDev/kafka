import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { createProducer } from '../../../src/producer/index';
import {
  createCluster,
  createModPartitioner,
  createTopic,
  generateMessages,
  newLogger,
  saslBrokers,
  saslEntries,
  secureRandom,
  sslBrokers,
  sslConnectionOpts,
  testIfKafkaAtLeast_0_11,
} from '../../helpers/index';

describe('producer.send', () => {
  let topicName: string;
  let producer: ReturnType<typeof createProducer> | undefined;

  beforeEach(() => {
    topicName = `test-topic-${secureRandom()}`;
  });

  afterEach(async () => {
    await producer?.disconnect();
  });

  it('rejects invalid topic and messages', async () => {
    producer = createProducer({ cluster: createCluster(), logger: newLogger() });
    await expect(producer.send({ acks: 1, topic: null as unknown as string, messages: [] })).rejects.toHaveProperty(
      'message',
      'Invalid topic',
    );
    await expect(producer.send({ acks: 1, topic: topicName, messages: null as unknown as [] })).rejects.toThrow(
      /Invalid messages array/,
    );
  });

  it('rejects send when disconnected', async () => {
    producer = createProducer({ cluster: createCluster(), logger: newLogger() });
    await expect(producer.send({ topic: topicName, messages: [{ key: 'k', value: 'v' }] })).rejects.toThrow(
      /The producer is disconnected/,
    );
  });

  it('returns no metadata when acks is 0', async () => {
    await createTopic({ topic: topicName });
    producer = createProducer({
      cluster: createCluster(),
      createPartitioner: createModPartitioner,
      logger: newLogger(),
    });
    await producer.connect();
    await expect(
      producer.send({ acks: 0, topic: topicName, messages: [{ key: 'k', value: 'fire-and-forget' }] }),
    ).resolves.toEqual([]);
  });

  it('requires ISR acknowledgements when acks is -1', async () => {
    await createTopic({ topic: topicName });
    producer = createProducer({
      cluster: createCluster(),
      createPartitioner: createModPartitioner,
      logger: newLogger(),
    });
    await producer.connect();
    await expect(
      producer.send({ acks: -1, timeout: 10_000, topic: topicName, messages: [{ key: 'k', value: 'all-isr' }] }),
    ).resolves.toEqual(
      expect.arrayContaining([expect.objectContaining({ topicName, partition: 0, errorCode: 0, baseOffset: 0n })]),
    );
  });

  it('returns immediately for an empty messages array', async () => {
    await createTopic({ topic: topicName });
    producer = createProducer({ cluster: createCluster(), logger: newLogger() });
    await producer.connect();
    await expect(producer.send({ acks: 1, topic: topicName, messages: [] })).resolves.toEqual([]);
  });

  it('rejects send when the signal is already aborted', async () => {
    await createTopic({ topic: topicName });
    producer = createProducer({ cluster: createCluster(), logger: newLogger() });
    await producer.connect();
    await expect(
      producer.send({
        acks: 1,
        topic: topicName,
        messages: [{ key: 'k', value: 'v' }],
        signal: AbortSignal.abort(),
      }),
    ).rejects.toThrow(/aborted/i);
  });

  it('sends messages and allows tombstones', async () => {
    await createTopic({ topic: topicName });
    producer = createProducer({
      cluster: createCluster(),
      createPartitioner: createModPartitioner,
      logger: newLogger(),
    });
    await producer.connect();
    const metadata = await producer.send({
      acks: 1,
      topic: topicName,
      messages: [...generateMessages({ number: 3 }), { key: 'tombstone', value: null }],
    });
    expect(metadata).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          topicName,
          partition: expect.any(Number),
          errorCode: 0,
          baseOffset: expect.any(BigInt),
        }),
      ]),
    );
  });

  testIfKafkaAtLeast_0_11('sends messages without a key', async () => {
    await createTopic({ topic: topicName });
    producer = createProducer({
      cluster: createCluster(),
      createPartitioner: createModPartitioner,
      logger: newLogger(),
    });
    await producer.connect();
    await expect(
      producer.send({
        acks: 1,
        topic: topicName,
        messages: [{ value: 'test-value' }],
      }),
    ).resolves.toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          topicName,
          partition: expect.any(Number),
          errorCode: 0,
          baseOffset: 0n,
        }),
      ]),
    );
  });

  testIfKafkaAtLeast_0_11('sends messages with headers', async () => {
    await createTopic({ topic: topicName });
    producer = createProducer({
      cluster: createCluster(),
      createPartitioner: createModPartitioner,
      logger: newLogger(),
    });
    await producer.connect();

    const sendMessages = () =>
      producer!.send({
        acks: 1,
        topic: topicName,
        messages: Array.from({ length: 10 }, (_, i) => ({
          key: `key-${i}`,
          value: `value-${i}`,
          headers: {
            [`header-a${i}`]: `header-value-a${i}`,
            [`header-b${i}`]: `header-value-b${i}`,
            [`header-c${i}`]: `header-value-c${i}`,
          },
        })),
      });

    await expect(sendMessages()).resolves.toEqual(
      expect.arrayContaining([expect.objectContaining({ topicName, partition: 0, errorCode: 0, baseOffset: 0n })]),
    );
    await expect(sendMessages()).resolves.toEqual(
      expect.arrayContaining([expect.objectContaining({ topicName, partition: 0, errorCode: 0, baseOffset: 10n })]),
    );
  });

  it('connects over SSL', async () => {
    producer = createProducer({ cluster: createCluster(sslConnectionOpts(), sslBrokers()), logger: newLogger() });
    await expect(producer.connect()).resolves.toBeUndefined();
  });

  it.each(saslEntries)('connects over SASL $name', async (entry) => {
    producer = createProducer({ cluster: createCluster(entry.opts(), saslBrokers()), logger: newLogger() });
    await expect(producer.connect()).resolves.toBeUndefined();
  });

  it.each(saslEntries.filter((e) => e.wrongOpts))('rejects failed SASL $name authentication', async (entry) => {
    producer = createProducer({ cluster: createCluster(entry.wrongOpts!(), saslBrokers()), logger: newLogger() });
    await expect(producer.connect()).rejects.toThrow(entry.expectedErr);
  });
});
