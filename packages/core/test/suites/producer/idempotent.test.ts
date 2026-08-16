import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { createConsumer } from '../../../src/consumer/index.js';
import { createProducer } from '../../../src/producer/index.js';
import {
  createCluster,
  createTopic,
  newLogger,
  secureRandom,
  waitForConsumerToJoinGroup,
  waitForMessages,
} from '../../helpers/index.js';

describe('producer.idempotent', () => {
  let topicName: string;
  let producer: ReturnType<typeof createProducer> | undefined;
  let consumer: ReturnType<typeof createConsumer> | undefined;

  beforeEach(async () => {
    topicName = `test-topic-${secureRandom()}`;
    await createTopic({ topic: topicName, partitions: 1 });
    producer = createProducer({ cluster: createCluster(), logger: newLogger(), idempotent: true });
    consumer = createConsumer({
      cluster: createCluster(),
      groupId: `group-${secureRandom()}`,
      maxWaitTimeInMs: 100,
      logger: newLogger(),
    });
    await producer.connect();
    await consumer.connect();
    await consumer.subscribe({ topic: topicName, fromBeginning: true });
  });

  afterEach(async () => {
    await consumer?.disconnect();
    await producer?.disconnect();
  });

  it('writes sequential messages once and in order', async () => {
    expect(producer!.isIdempotent()).toBe(true);
    const messages = Array.from({ length: 4 }, (_, i) => ({ key: `k-${i}`, value: `${i}` }));
    for (const message of messages) {
      await producer!.send({ acks: -1, topic: topicName, messages: [message] });
    }

    const consumed: { message: { value: Buffer | null } }[] = [];
    const join = waitForConsumerToJoinGroup(consumer!);
    await consumer!.run({
      eachMessage: async (event) => {
        consumed.push(event);
      },
    });
    await join;
    await waitForMessages(consumed, { number: messages.length });
    expect(consumed.map((c) => c.message.value?.toString())).toEqual(['0', '1', '2', '3']);
  });
});
