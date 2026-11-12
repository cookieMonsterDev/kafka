import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { createAdmin } from '../../../src/admin/index';
import { createConsumer } from '../../../src/consumer/index';
import { createProducer } from '../../../src/producer/index';
import type { BatchProcessPayload, CommitOffsetsPayload } from '../../../src/consumer/instrumentation-events';
import type { EachMessagePayload } from '../../../src/consumer/types';
import {
  createCluster,
  createTopic,
  generateMessages,
  newLogger,
  secureRandom,
  waitFor,
  waitForConsumerToJoinGroup,
  waitForMessages,
} from '../../helpers/index';

describe('consumer.autoCommit', () => {
  let topicName: string;
  let groupId: string;
  let producer: ReturnType<typeof createProducer> | undefined;
  let consumer: ReturnType<typeof createConsumer> | undefined;
  let admin: ReturnType<typeof createAdmin> | undefined;

  beforeEach(async () => {
    topicName = `test-topic-${secureRandom()}`;
    groupId = `group-${secureRandom()}`;
    await createTopic({ topic: topicName });
    producer = createProducer({ cluster: createCluster(), logger: newLogger() });
    admin = createAdmin({ cluster: createCluster(), logger: newLogger() });
  });

  afterEach(async () => {
    await consumer?.disconnect();
    await producer?.disconnect();
    await admin?.disconnect();
  });

  it('does not persist offsets when autoCommit is false', async () => {
    consumer = createConsumer({
      cluster: createCluster(),
      groupId,
      maxWaitTimeInMs: 100,
      logger: newLogger(),
    });
    await consumer.connect();
    await producer!.connect();
    await admin!.connect();
    await consumer.subscribe({ topic: topicName, fromBeginning: true });

    const firstPass: EachMessagePayload[] = [];
    const join = waitForConsumerToJoinGroup(consumer);
    await consumer.run({
      autoCommit: false,
      eachMessage: async (event) => {
        firstPass.push(event);
      },
    });
    await join;
    await producer!.send({ acks: 1, topic: topicName, messages: generateMessages({ number: 4 }) });
    await waitForMessages(firstPass, { number: 4 });
    await consumer.disconnect();
    consumer = undefined;

    const committed = await admin!.fetchOffsets({ groupId, topics: [topicName] });
    expect(committed[0]?.partitions[0]?.offset).toBe(-1n);

    consumer = createConsumer({
      cluster: createCluster(),
      groupId,
      maxWaitTimeInMs: 100,
      logger: newLogger(),
    });
    await consumer.connect();
    await consumer.subscribe({ topic: topicName, fromBeginning: true });
    const secondPass: bigint[] = [];
    const rejoin = waitForConsumerToJoinGroup(consumer);
    await consumer.run({
      autoCommit: false,
      eachMessage: async (event) => {
        secondPass.push(event.message.offset);
      },
    });
    await rejoin;
    await waitForMessages(secondPass, { number: 4 });
    expect(secondPass).toEqual([0n, 1n, 2n, 3n]);
  });

  it('commits once per fetched batch by default, not between maxRecords slices', async () => {
    const messageCount = 1200;
    await producer!.connect();
    await producer!.send({ acks: 1, topic: topicName, messages: generateMessages({ number: messageCount }) });

    consumer = createConsumer({
      cluster: createCluster(),
      groupId,
      maxWaitTimeInMs: 100,
      logger: newLogger(),
    });
    await consumer.connect();
    await consumer.subscribe({ topic: topicName, fromBeginning: true });

    const batchEndOffsets = new Set<bigint>();
    const batchSizes: number[] = [];
    const committedOffsets: bigint[] = [];
    consumer.on(consumer.events.END_BATCH_PROCESS, (event) => {
      const { lastOffset, batchSize } = event.payload as BatchProcessPayload;
      batchEndOffsets.add(lastOffset + 1n);
      batchSizes.push(batchSize);
    });
    consumer.on(consumer.events.COMMIT_OFFSETS, (event) => {
      for (const { partitions } of (event.payload as CommitOffsetsPayload).topics) {
        for (const { offset } of partitions) committedOffsets.push(offset);
      }
    });

    const consumed: bigint[] = [];
    await consumer.run({
      eachMessage: async ({ message }) => {
        consumed.push(message.offset);
      },
    });
    await waitForMessages(consumed, { number: messageCount });
    await waitFor(() => committedOffsets.includes(BigInt(messageCount)));

    // At least one fetched batch holds more than the default 500-record `maxRecords` slice, so a
    // commit between slices would land on an offset no batch ended at.
    expect(Math.max(...batchSizes)).toBeGreaterThan(500);
    expect(committedOffsets.every((offset) => batchEndOffsets.has(offset))).toBe(true);
  });

  it('commits after autoCommitThreshold messages', async () => {
    consumer = createConsumer({
      cluster: createCluster(),
      groupId,
      maxWaitTimeInMs: 50,
      logger: newLogger(),
    });
    await consumer.connect();
    await producer!.connect();
    await admin!.connect();
    await consumer.subscribe({ topic: topicName, fromBeginning: true });

    const consumed: EachMessagePayload[] = [];
    const join = waitForConsumerToJoinGroup(consumer);
    await consumer.run({
      autoCommitInterval: 60_000,
      autoCommitThreshold: 3,
      eachMessage: async (event) => {
        consumed.push(event);
      },
    });
    await join;
    await producer!.send({ acks: 1, topic: topicName, messages: generateMessages({ number: 1 }) });
    await waitForMessages(consumed, { number: 1 });

    const beforeThreshold = await admin!.fetchOffsets({ groupId, topics: [topicName] });
    expect(beforeThreshold[0]?.partitions[0]?.offset).toBe(-1n);

    await producer!.send({ acks: 1, topic: topicName, messages: generateMessages({ number: 2 }) });
    await waitForMessages(consumed, { number: 3 });

    const committed = await waitFor(async () => {
      const offsets = await admin!.fetchOffsets({ groupId, topics: [topicName] });
      const offset = offsets[0]?.partitions[0]?.offset;
      return offset === 3n ? offset : false;
    });
    expect(committed).toBe(3n);
  });
});
