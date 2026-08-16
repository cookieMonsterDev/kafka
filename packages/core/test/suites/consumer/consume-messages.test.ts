import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { createConsumer } from '../../../src/consumer/index';
import { createProducer } from '../../../src/producer/index';
import type { EachMessagePayload } from '../../../src/consumer/types';
import {
  createCluster,
  createModPartitioner,
  createTopic,
  generateMessages,
  newLogger,
  secureRandom,
  waitForConsumerToJoinGroup,
  waitForMessages,
} from '../../helpers/index';

describe('consumer.consumeMessages', () => {
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

  it('consumes messages from the beginning', async () => {
    await consumer!.connect();
    await producer!.connect();
    await consumer!.subscribe({ topic: topicName, fromBeginning: true });
    const consumed: EachMessagePayload[] = [];
    const join = waitForConsumerToJoinGroup(consumer!);
    await consumer!.run({
      eachMessage: async (event) => {
        consumed.push(event);
      },
    });
    await join;

    const messages = generateMessages({ number: 20 });
    await producer!.send({ acks: 1, topic: topicName, messages });
    await waitForMessages(consumed, { number: messages.length });

    expect(consumed[0]?.message.offset).toBe(0n);
    expect(consumed[consumed.length - 1]?.message.offset).toBe(19n);
    expect(consumed.map((m) => m.message.offset)).toEqual(messages.map((_, i) => BigInt(i)));
    expect(consumed[0]?.message.key?.toString()).toBe(messages[0]!.key);
    expect(consumed[0]?.message.value?.toString()).toBe(messages[0]!.value);
  });

  it('consumes via eachBatch', async () => {
    await consumer!.connect();
    await producer!.connect();
    await consumer!.subscribe({ topic: topicName, fromBeginning: true });
    const consumed: { offset: bigint }[] = [];
    const join = waitForConsumerToJoinGroup(consumer!);
    await consumer!.run({
      eachBatch: async ({ batch }) => {
        for (const message of batch.messages) consumed.push({ offset: message.offset });
      },
    });
    await join;
    await producer!.send({ acks: 1, topic: topicName, messages: generateMessages({ number: 5 }) });
    await waitForMessages(consumed, { number: 5 });
    expect(consumed.map((m) => m.offset)).toEqual([0n, 1n, 2n, 3n, 4n]);
  });
});
