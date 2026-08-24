import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { createAdmin } from '../../../src/admin/index';
import { EARLIEST_OFFSET } from '../../../src/constants';
import { createConsumer } from '../../../src/consumer/index';
import { createProducer } from '../../../src/producer/index';
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

describe('admin.offsets', () => {
  let topicName: string;
  let groupId: string;
  let admin: ReturnType<typeof createAdmin> | undefined;
  let producer: ReturnType<typeof createProducer> | undefined;
  let consumer: ReturnType<typeof createConsumer> | undefined;

  beforeEach(async () => {
    topicName = `test-topic-${secureRandom()}`;
    groupId = `test-group-${secureRandom()}`;
    await createTopic({ topic: topicName, partitions: 1 });
  });

  afterEach(async () => {
    await consumer?.disconnect();
    await producer?.disconnect();
    await admin?.disconnect();
  });

  it('fetches topic offsets and group offsets', async () => {
    const cluster = createCluster();
    admin = createAdmin({ cluster, logger: newLogger() });
    producer = createProducer({ cluster, createPartitioner: createModPartitioner, logger: newLogger() });

    await admin.connect();
    await producer.connect();

    const messages = generateMessages({ number: 10 });
    await producer.send({ acks: 1, topic: topicName, messages });

    const topicOffsets = await admin.fetchTopicOffsets(topicName);
    expect(topicOffsets).toEqual([{ partition: 0, offset: 10n, low: 0n, high: 10n }]);

    consumer = createConsumer({
      cluster: createCluster(),
      groupId,
      maxWaitTimeInMs: 100,
      logger: newLogger(),
    });
    await consumer.connect();
    await consumer.subscribe({ topic: topicName, fromBeginning: true });
    const consumed: unknown[] = [];
    const join = waitForConsumerToJoinGroup(consumer);
    await consumer.run({
      eachMessage: async (event) => {
        consumed.push(event);
      },
    });
    await join;
    await waitForMessages(consumed, { number: 10 });
    await consumer.disconnect();
    consumer = undefined;

    const groupOffsets = await admin.fetchOffsets({ groupId, topics: [topicName] });
    const partition = groupOffsets.find((t) => t.topic === topicName)?.partitions[0];
    expect(partition?.offset).toBe(10n);
  });

  it('resets offsets to earliest', async () => {
    const cluster = createCluster();
    admin = createAdmin({ cluster, logger: newLogger() });
    producer = createProducer({ cluster, logger: newLogger() });
    await admin.connect();
    await producer.connect();
    await producer.send({ acks: 1, topic: topicName, messages: generateMessages({ number: 5 }) });

    await admin.setOffsets({
      groupId,
      topic: topicName,
      partitions: [{ partition: 0, offset: 3n }],
    });
    await admin.resetOffsets({ groupId, topic: topicName, earliest: true });
    const offsets = await admin.fetchOffsets({ groupId, topics: [topicName] });
    expect(offsets[0]?.partitions[0]?.offset).toBe(BigInt(EARLIEST_OFFSET));
  });

  it('resets offsets to latest and resolves them to the high watermark', async () => {
    const cluster = createCluster();
    admin = createAdmin({ cluster, logger: newLogger() });
    producer = createProducer({ cluster, logger: newLogger() });
    await admin.connect();
    await producer.connect();
    await producer.send({ acks: 1, topic: topicName, messages: generateMessages({ number: 5 }) });

    await admin.resetOffsets({ groupId, topic: topicName, earliest: false });
    const sentinels = await admin.fetchOffsets({ groupId, topics: [topicName] });
    expect(sentinels[0]?.partitions[0]?.offset).toBe(-1n);

    const resolved = await admin.fetchOffsets({ groupId, topics: [topicName], resolveOffsets: true });
    expect(resolved[0]?.partitions[0]?.offset).toBe(5n);
  });

  it('looks up offsets by timestamp and falls back to the high watermark', async () => {
    const cluster = createCluster();
    admin = createAdmin({ cluster, logger: newLogger() });
    producer = createProducer({ cluster, logger: newLogger() });
    await admin.connect();
    await producer.connect();
    await producer.send({ acks: 1, topic: topicName, messages: generateMessages({ number: 5 }) });

    const fromStart = await admin.fetchTopicOffsetsByTimestamp(topicName, 0n);
    expect(fromStart).toEqual([{ partition: 0, offset: 0n }]);

    const fromFuture = await admin.fetchTopicOffsetsByTimestamp(topicName, 4_102_444_800_000n);
    expect(fromFuture).toEqual([{ partition: 0, offset: 5n }]);
  });

  it('sets a committed offset that a new consumer member picks up', async () => {
    const cluster = createCluster();
    admin = createAdmin({ cluster, logger: newLogger() });
    producer = createProducer({ cluster, logger: newLogger() });
    await admin.connect();
    await producer.connect();
    await producer.send({ acks: 1, topic: topicName, messages: generateMessages({ number: 10 }) });
    await admin.setOffsets({ groupId, topic: topicName, partitions: [{ partition: 0, offset: 7n }] });

    consumer = createConsumer({
      cluster: createCluster(),
      groupId,
      maxWaitTimeInMs: 100,
      logger: newLogger(),
    });
    await consumer.connect();
    await consumer.subscribe({ topic: topicName });
    const consumed: { offset: bigint }[] = [];
    const join = waitForConsumerToJoinGroup(consumer);
    await consumer.run({
      eachMessage: async (event) => {
        consumed.push({ offset: event.message.offset });
      },
    });
    await join;
    await waitForMessages(consumed, { number: 3 });
    expect(consumed.map((entry) => entry.offset)).toEqual([7n, 8n, 9n]);
  });

  it('returns the unknown-offset sentinel for a group that has never committed', async () => {
    const cluster = createCluster();
    admin = createAdmin({ cluster, logger: newLogger() });
    await admin.connect();
    const offsets = await admin.fetchOffsets({ groupId, topics: [topicName] });
    expect(offsets[0]?.partitions[0]).toEqual(expect.objectContaining({ partition: 0, offset: -1n }));
  });
});
