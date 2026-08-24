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
  testIfKafkaAtLeast_0_11,
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

  testIfKafkaAtLeast_0_11('consumes RecordBatch messages with headers', async () => {
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

    await producer!.send({
      acks: 1,
      topic: topicName,
      messages: [
        {
          key: 'key-0',
          value: 'value-0',
          headers: { 'header-a': 'header-value-a', 'header-b': 'header-value-b' },
        },
      ],
    });
    await waitForMessages(consumed, { number: 1 });

    const headers = consumed[0]?.message.headers ?? {};
    const headerValue = (value: (typeof headers)[string] | undefined) => {
      const first = Array.isArray(value) ? value[0] : value;
      return first?.toString();
    };
    expect(consumed[0]?.message.value?.toString()).toBe('value-0');
    expect(headerValue(headers['header-a'])).toBe('header-value-a');
    expect(headerValue(headers['header-b'])).toBe('header-value-b');
  });

  it('consumes batches through stream()', async () => {
    await consumer!.connect();
    await producer!.connect();
    await consumer!.subscribe({ topic: topicName, fromBeginning: true });
    const join = waitForConsumerToJoinGroup(consumer!);
    const offsets: bigint[] = [];
    const iterating = (async () => {
      for await (const batch of consumer!.stream()) {
        for (const message of batch.messages) offsets.push(message.offset);
        if (offsets.length >= 5) break;
      }
    })();
    await join;
    await producer!.send({ acks: 1, topic: topicName, messages: generateMessages({ number: 5 }) });
    await iterating;
    expect(offsets).toEqual([0n, 1n, 2n, 3n, 4n]);
  });

  it('rejects stream() while run() is already active', async () => {
    await consumer!.connect();
    await consumer!.subscribe({ topic: topicName, fromBeginning: true });
    const join = waitForConsumerToJoinGroup(consumer!);
    await consumer!.run({ eachMessage: async () => undefined });
    await join;
    await expect(consumer!.stream().next()).rejects.toThrow(/already running/);
  });

  it('consumes tombstones as null values', async () => {
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
    await producer!.send({ acks: 1, topic: topicName, messages: [{ key: 'gone', value: null }] });
    await waitForMessages(consumed, { number: 1 });
    expect(consumed[0]?.message.key?.toString()).toBe('gone');
    expect(consumed[0]?.message.value).toBeNull();
  });

  it('consumes from several subscribed topics', async () => {
    const otherTopic = `test-topic-${secureRandom()}`;
    await createTopic({ topic: otherTopic });
    await consumer!.connect();
    await producer!.connect();
    await consumer!.subscribe({ topics: [topicName, otherTopic], fromBeginning: true });
    const consumed: EachMessagePayload[] = [];
    const join = waitForConsumerToJoinGroup(consumer!);
    await consumer!.run({
      eachMessage: async (event) => {
        consumed.push(event);
      },
    });
    await join;
    await producer!.send({ acks: 1, topic: topicName, messages: [{ key: 'a', value: 'one' }] });
    await producer!.send({ acks: 1, topic: otherTopic, messages: [{ key: 'b', value: 'two' }] });
    await waitForMessages(consumed, { number: 2 });
    expect(new Set(consumed.map((event) => event.topic))).toEqual(new Set([topicName, otherTopic]));
  });

  it('resumes fetching after stop() then run() again', async () => {
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
    await producer!.send({ acks: 1, topic: topicName, messages: generateMessages({ number: 2 }) });
    await waitForMessages(consumed, { number: 2 });

    await consumer!.stop();
    const rejoin = waitForConsumerToJoinGroup(consumer!);
    await consumer!.run({
      eachMessage: async (event) => {
        consumed.push(event);
      },
    });
    await rejoin;
    await producer!.send({ acks: 1, topic: topicName, messages: generateMessages({ number: 2 }) });
    await waitForMessages(consumed, { number: 4 });
    expect(consumed.map((event) => event.message.offset)).toEqual([0n, 1n, 2n, 3n]);
  });
});
