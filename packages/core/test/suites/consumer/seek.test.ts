import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { createConsumer } from '../../../src/consumer/index.js';
import { createProducer } from '../../../src/producer/index.js';
import { KafkaJSNonRetriableError } from '../../../src/errors.js';
import type { EachMessagePayload } from '../../../src/consumer/types.js';
import {
  createCluster,
  createModPartitioner,
  createTopic,
  generateMessages,
  newLogger,
  secureRandom,
  waitForConsumerToJoinGroup,
  waitForMessages,
} from '../../helpers/index.js';

describe('consumer.seek', () => {
  let topicName: string;
  let producer: ReturnType<typeof createProducer> | undefined;
  let consumer: ReturnType<typeof createConsumer> | undefined;

  beforeEach(async () => {
    topicName = `test-topic-${secureRandom()}`;
    await createTopic({ topic: topicName, partitions: 1 });
    producer = createProducer({
      cluster: createCluster(),
      createPartitioner: createModPartitioner,
      logger: newLogger(),
    });
    consumer = createConsumer({
      cluster: createCluster(),
      groupId: `group-${secureRandom()}`,
      maxWaitTimeInMs: 100,
      logger: newLogger(),
    });
  });

  afterEach(async () => {
    await consumer?.disconnect();
    await producer?.disconnect();
  });

  it('rejects seek before run', () => {
    expect(() => consumer!.seek({ topic: topicName, partition: 0, offset: 1n })).toThrow(KafkaJSNonRetriableError);
  });

  it('seeks to an absolute offset', async () => {
    await producer!.connect();
    await producer!.send({ acks: 1, topic: topicName, messages: generateMessages({ number: 10 }) });

    await consumer!.connect();
    await consumer!.subscribe({ topic: topicName, fromBeginning: true });
    const consumed: EachMessagePayload[] = [];
    const join = waitForConsumerToJoinGroup(consumer!);
    await consumer!.run({
      eachMessage: async (event) => {
        consumed.push(event);
      },
    });
    await join;
    consumer!.seek({ topic: topicName, partition: 0, offset: 7n });
    await waitForMessages(consumed, { number: 3 });
    expect(consumed[0]?.message.offset).toBe(7n);
  });
});
