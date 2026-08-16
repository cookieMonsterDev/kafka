import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { createAdmin } from '../../src/admin/index.js';
import { createConsumer } from '../../src/consumer/index.js';
import { createProducer } from '../../src/producer/index.js';
import { KafkaJSNonRetriableError } from '../../src/errors.js';
import {
  createCluster,
  createModPartitioner,
  createTopic,
  generateMessages,
  newLogger,
  secureRandom,
  waitForConsumerToJoinGroup,
  waitForMessages,
} from '../helpers/index.js';

describe('consumer.commitOffsets', () => {
  let topicName: string;
  let groupId: string;
  let producer: ReturnType<typeof createProducer> | undefined;
  let consumer: ReturnType<typeof createConsumer> | undefined;
  let admin: ReturnType<typeof createAdmin> | undefined;

  beforeEach(async () => {
    topicName = `test-topic-${secureRandom()}`;
    groupId = `group-${secureRandom()}`;
    await createTopic({ topic: topicName });
    producer = createProducer({
      cluster: createCluster(),
      createPartitioner: createModPartitioner,
      logger: newLogger(),
    });
    consumer = createConsumer({
      cluster: createCluster(),
      groupId,
      maxWaitTimeInMs: 100,
      logger: newLogger(),
    });
    admin = createAdmin({ cluster: createCluster(), logger: newLogger() });
  });

  afterEach(async () => {
    await consumer?.disconnect();
    await producer?.disconnect();
    await admin?.disconnect();
  });

  it('rejects commit before run', async () => {
    await expect(consumer!.commitOffsets([{ topic: topicName, partition: 0, offset: 1n }])).rejects.toThrow(
      KafkaJSNonRetriableError,
    );
  });

  it('commits consumed offsets', async () => {
    await consumer!.connect();
    await producer!.connect();
    await admin!.connect();
    await consumer!.subscribe({ topic: topicName, fromBeginning: true });
    const consumed: unknown[] = [];
    const join = waitForConsumerToJoinGroup(consumer!);
    await consumer!.run({ eachMessage: async (event) => consumed.push(event) });
    await join;
    await producer!.send({ acks: 1, topic: topicName, messages: generateMessages({ number: 5 }) });
    await waitForMessages(consumed, { number: 5 });
    await consumer!.commitOffsets([{ topic: topicName, partition: 0, offset: 5n }]);

    const offsets = await admin!.fetchOffsets({ groupId, topics: [topicName] });
    expect(offsets[0]?.partitions[0]?.offset).toBe(5n);
  });
});
