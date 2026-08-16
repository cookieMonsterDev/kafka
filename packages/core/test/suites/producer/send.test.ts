import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { createProducer } from '../../../src/producer/index.js';
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
} from '../../helpers/index.js';

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
