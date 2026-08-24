import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { createConsumer } from '../../../src/consumer/index';
import { createProducer } from '../../../src/producer/index';
import { KafkaNonRetriableError } from '../../../src/errors';
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
    expect(() => consumer!.seek({ topic: topicName, partition: 0, offset: 1n })).toThrow(KafkaNonRetriableError);
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

  it('accepts numeric and string offsets', async () => {
    await producer!.connect();
    await producer!.send({ acks: 1, topic: topicName, messages: generateMessages({ number: 5 }) });

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
    consumer!.seek({ topic: topicName, partition: 0, offset: 3 });
    await waitForMessages(consumed, { number: 2 });
    expect(consumed[0]?.message.offset).toBe(3n);

    consumed.length = 0;
    consumer!.seek({ topic: topicName, partition: 0, offset: '4' });
    await waitForMessages(consumed, { number: 1 });
    expect(consumed[0]?.message.offset).toBe(4n);
  });

  it('seeks backward after starting from the high watermark', async () => {
    await producer!.connect();
    await producer!.send({ acks: 1, topic: topicName, messages: generateMessages({ number: 3 }) });

    await consumer!.connect();
    await consumer!.subscribe({ topic: topicName, fromBeginning: false });
    const consumed: EachMessagePayload[] = [];
    const join = waitForConsumerToJoinGroup(consumer!);
    await consumer!.run({
      eachMessage: async (event) => {
        consumed.push(event);
      },
    });
    await join;
    consumer!.seek({ topic: topicName, partition: 0, offset: 0n });
    await waitForMessages(consumed, { number: 3 });
    expect(consumed.map((event) => event.message.offset)).toEqual([0n, 1n, 2n]);
  });
});
