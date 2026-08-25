import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { createConsumer } from '../../../src/consumer/index';
import { createProducer } from '../../../src/producer/index';
import type { EachMessagePayload } from '../../../src/consumer/types';
import {
  createCluster,
  createTopic,
  generateMessages,
  newLogger,
  secureRandom,
  testIfKafkaAtLeast_4_0,
  waitForConsumerToJoinGroup,
  waitForMessages,
  waitForNextEvent,
} from '../../helpers/index';

describe('consumer.offsetReset', () => {
  let topicName: string;
  let groupId: string;
  let producer: ReturnType<typeof createProducer> | undefined;
  let consumer: ReturnType<typeof createConsumer> | undefined;

  beforeEach(async () => {
    topicName = `test-topic-${secureRandom()}`;
    groupId = `group-${secureRandom()}`;
    await createTopic({ topic: topicName });
    producer = createProducer({ cluster: createCluster(), logger: newLogger() });
  });

  afterEach(async () => {
    await consumer?.disconnect();
    await producer?.disconnect();
  });

  it('starts at the high watermark when fromBeginning is omitted', async () => {
    await producer!.connect();
    await producer!.send({ acks: 1, topic: topicName, messages: generateMessages({ number: 10 }) });

    consumer = createConsumer({
      cluster: createCluster(),
      groupId,
      maxWaitTimeInMs: 100,
      logger: newLogger(),
    });
    await consumer.connect();
    await consumer.subscribe({ topic: topicName });
    const consumed: EachMessagePayload[] = [];
    const join = waitForConsumerToJoinGroup(consumer);
    const fetched = waitForNextEvent(consumer, consumer.events.FETCH);
    await consumer.run({
      eachMessage: async (event) => {
        consumed.push(event);
      },
    });
    await join;
    await fetched;

    await producer!.send({ acks: 1, topic: topicName, messages: generateMessages({ number: 5 }) });
    await waitForMessages(consumed, { number: 5 });
    expect(consumed.map((event) => event.message.offset)).toEqual([10n, 11n, 12n, 13n, 14n]);
  });

  it('starts at the log start when autoOffsetReset is earliest', async () => {
    await producer!.connect();
    await producer!.send({ acks: 1, topic: topicName, messages: generateMessages({ number: 4 }) });

    consumer = createConsumer({
      cluster: createCluster(),
      groupId,
      maxWaitTimeInMs: 100,
      logger: newLogger(),
    });
    await consumer.connect();
    await consumer.subscribe({ topic: topicName, autoOffsetReset: 'earliest' });
    const consumed: EachMessagePayload[] = [];
    const join = waitForConsumerToJoinGroup(consumer);
    await consumer.run({
      eachMessage: async (event) => {
        consumed.push(event);
      },
    });
    await join;
    await waitForMessages(consumed, { number: 4 });
    expect(consumed[0]?.message.offset).toBe(0n);
    expect(consumed[consumed.length - 1]?.message.offset).toBe(3n);
  });

  it('crashes when autoOffsetReset is none and the group has no committed offset', async () => {
    consumer = createConsumer({
      cluster: createCluster(),
      groupId,
      maxWaitTimeInMs: 100,
      retry: { retries: 0, restartOnFailure: async () => false },
      logger: newLogger(),
    });
    await consumer.connect();
    await consumer.subscribe({ topic: topicName, autoOffsetReset: 'none' });

    const crashed = new Promise<Error>((resolve) => {
      consumer!.on(consumer!.events.CRASH, (event) => {
        resolve((event.payload as { error: Error }).error);
      });
    });
    await consumer.run({ eachMessage: async () => undefined });
    await expect(crashed).resolves.toMatchObject({
      message: expect.stringMatching(/Offset reset policy is none/),
    });
  });

  testIfKafkaAtLeast_4_0(
    'starts at the log start when autoOffsetReset is by_duration and the timestamp is before the log',
    async () => {
      await producer!.connect();
      await producer!.send({ acks: 1, topic: topicName, messages: generateMessages({ number: 4 }) });

      consumer = createConsumer({
        cluster: createCluster(),
        groupId,
        maxWaitTimeInMs: 100,
        logger: newLogger(),
      });
      await consumer.connect();
      await consumer.subscribe({ topic: topicName, autoOffsetReset: 'by_duration:P10D' });
      const consumed: EachMessagePayload[] = [];
      const join = waitForConsumerToJoinGroup(consumer);
      await consumer.run({
        eachMessage: async (event) => {
          consumed.push(event);
        },
      });
      await join;
      await waitForMessages(consumed, { number: 4 });
      expect(consumed[0]?.message.offset).toBe(0n);
      expect(consumed[consumed.length - 1]?.message.offset).toBe(3n);
    },
  );
});
