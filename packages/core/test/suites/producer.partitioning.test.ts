import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { createConsumer } from '../../src/consumer/index.js';
import { createProducer } from '../../src/producer/index.js';
import { Partitioners } from '../../src/index.js';
import {
  createCluster,
  createModPartitioner,
  createTopic,
  newLogger,
  secureRandom,
  waitForConsumerToJoinGroup,
  waitForMessages,
} from '../helpers/index.js';

describe('producer.partitioning', () => {
  let topicName: string;
  let producer: ReturnType<typeof createProducer> | undefined;
  let consumer: ReturnType<typeof createConsumer> | undefined;

  beforeEach(async () => {
    topicName = `test-topic-${secureRandom()}`;
    await createTopic({ topic: topicName, partitions: 3 });
  });

  afterEach(async () => {
    await consumer?.disconnect();
    await producer?.disconnect();
  });

  it('honors an explicit partition', async () => {
    const cluster = createCluster();
    producer = createProducer({ cluster, createPartitioner: createModPartitioner, logger: newLogger() });
    consumer = createConsumer({
      cluster: createCluster(),
      groupId: `group-${secureRandom()}`,
      maxWaitTimeInMs: 100,
      logger: newLogger(),
    });
    await producer.connect();
    await consumer.connect();
    await consumer.subscribe({ topic: topicName, fromBeginning: true });
    const consumed: { partition: number }[] = [];
    const join = waitForConsumerToJoinGroup(consumer);
    await consumer.run({ eachMessage: async (event) => consumed.push(event) });
    await join;

    await producer.send({
      acks: 1,
      topic: topicName,
      messages: [{ key: 'k', value: 'v', partition: 2 }],
    });
    await waitForMessages(consumed, { number: 1 });
    expect(consumed[0]?.partition).toBe(2);
  });

  it('uses the default partitioner when none is provided', async () => {
    producer = createProducer({
      cluster: createCluster(),
      createPartitioner: Partitioners.DefaultPartitioner,
      logger: newLogger(),
    });
    await producer.connect();
    const metadata = await producer.send({
      acks: 1,
      topic: topicName,
      messages: [{ key: 'same-key', value: 'a' }, { key: 'same-key', value: 'b' }],
    });
    expect(new Set(metadata.map((m) => m.partition)).size).toBe(1);
  });
});
