import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { createConsumer } from '../../../src/consumer/index';
import { createProducer } from '../../../src/producer/index';
import { COMPRESSION_TYPES } from '../../../src/protocol/compression/index';
import {
  createCluster,
  createModPartitioner,
  createTopic,
  newLogger,
  secureRandom,
  testIfKafkaAtLeast_2_1,
  waitForConsumerToJoinGroup,
  waitForMessages,
} from '../../helpers/index';

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

  async function roundTrip(compression: (typeof COMPRESSION_TYPES)[keyof typeof COMPRESSION_TYPES]): Promise<void> {
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
  }

  it('round-trips gzip-compressed messages', async () => {
    expect.assertions(1);
    await roundTrip(COMPRESSION_TYPES.GZIP);
  });

  it('round-trips snappy-compressed messages', async () => {
    expect.assertions(1);
    await roundTrip(COMPRESSION_TYPES.Snappy);
  });

  it('round-trips lz4-compressed messages', async () => {
    expect.assertions(1);
    await roundTrip(COMPRESSION_TYPES.LZ4);
  });

  testIfKafkaAtLeast_2_1('round-trips zstd-compressed messages', async () => {
    expect.assertions(1);
    await roundTrip(COMPRESSION_TYPES.ZSTD);
  });
});
