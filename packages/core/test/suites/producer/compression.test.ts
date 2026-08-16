import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { createConsumer } from '../../../src/consumer/index.js';
import { createProducer } from '../../../src/producer/index.js';
import { COMPRESSION_TYPES } from '../../../src/protocol/compression/index.js';
import { KafkaJSNotImplemented } from '../../../src/errors.js';
import {
  createCluster,
  createModPartitioner,
  createTopic,
  newLogger,
  secureRandom,
  waitForConsumerToJoinGroup,
  waitForMessages,
} from '../../helpers/index.js';

describe('producer.compression', () => {
  let topicName: string;
  let producer: ReturnType<typeof createProducer> | undefined;
  let consumer: ReturnType<typeof createConsumer> | undefined;

  beforeEach(async () => {
    topicName = `test-topic-${secureRandom()}`;
    await createTopic({ topic: topicName });
    producer = createProducer({
      cluster: createCluster(),
      createPartitioner: createModPartitioner,
      logger: newLogger(),
    });
    await producer.connect();
  });

  afterEach(async () => {
    await consumer?.disconnect();
    await producer?.disconnect();
  });

  it.each([
    ['gzip', COMPRESSION_TYPES.GZIP],
    ['zstd', COMPRESSION_TYPES.ZSTD],
  ] as const)('round-trips %s-compressed messages', async (_name, compression) => {
    consumer = createConsumer({
      cluster: createCluster(),
      groupId: `group-${secureRandom()}`,
      maxWaitTimeInMs: 100,
      logger: newLogger(),
    });
    await consumer.connect();
    await consumer.subscribe({ topic: topicName, fromBeginning: true });
    const consumed: { message: { value: Buffer | null } }[] = [];
    const join = waitForConsumerToJoinGroup(consumer);
    await consumer.run({
      eachMessage: async (event) => {
        consumed.push(event);
      },
    });
    await join;

    await producer!.send({
      acks: 1,
      compression,
      topic: topicName,
      messages: [{ key: 'k', value: 'compressed' }],
    });
    await waitForMessages(consumed, { number: 1 });
    expect(consumed[0]?.message.value?.toString()).toBe('compressed');
  });

  it('throws for unconfigured snappy and lz4', async () => {
    await expect(
      producer!.send({
        acks: 1,
        topic: topicName,
        compression: COMPRESSION_TYPES.Snappy,
        messages: [{ key: 'k', value: 'v' }],
      }),
    ).rejects.toThrow(KafkaJSNotImplemented);
    await expect(
      producer!.send({
        acks: 1,
        topic: topicName,
        compression: COMPRESSION_TYPES.LZ4,
        messages: [{ key: 'k', value: 'v' }],
      }),
    ).rejects.toThrow(KafkaJSNotImplemented);
  });
});
