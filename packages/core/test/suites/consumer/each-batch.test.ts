import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { createAdmin } from '../../../src/admin/index';
import { createConsumer } from '../../../src/consumer/index';
import { createProducer } from '../../../src/producer/index';
import type { EachBatchPayload } from '../../../src/consumer/types';
import {
  createCluster,
  createTopic,
  generateMessages,
  newLogger,
  secureRandom,
  waitForConsumerToJoinGroup,
  waitForMessages,
} from '../../helpers/index';

describe('consumer.eachBatch', () => {
  let topicName: string;
  let groupId: string;
  let producer: ReturnType<typeof createProducer> | undefined;
  let consumer: ReturnType<typeof createConsumer> | undefined;
  let admin: ReturnType<typeof createAdmin> | undefined;

  beforeEach(async () => {
    topicName = `test-topic-${secureRandom()}`;
    groupId = `group-${secureRandom()}`;
    await createTopic({ topic: topicName, partitions: 2 });
    producer = createProducer({ cluster: createCluster(), logger: newLogger() });
  });

  afterEach(async () => {
    await consumer?.disconnect();
    await producer?.disconnect();
    await admin?.disconnect();
  });

  it('exposes heartbeat, isRunning, and uncommittedOffsets while processing', async () => {
    consumer = createConsumer({
      cluster: createCluster(),
      groupId,
      maxWaitTimeInMs: 100,
      logger: newLogger(),
    });
    await consumer.connect();
    await producer!.connect();
    await consumer.subscribe({ topic: topicName, fromBeginning: true });

    const snapshots: {
      running: boolean;
      stale: boolean;
      uncommitted: bigint | undefined;
    }[] = [];
    const join = waitForConsumerToJoinGroup(consumer);
    await consumer.run({
      autoCommit: false,
      eachBatch: async ({
        batch,
        heartbeat,
        isRunning,
        isStale,
        uncommittedOffsets,
        resolveOffset,
      }: EachBatchPayload) => {
        await heartbeat();
        for (const message of batch.messages) resolveOffset(message.offset);
        const offsets = uncommittedOffsets();
        snapshots.push({
          running: isRunning(),
          stale: isStale(),
          uncommitted: offsets.topics.find((entry) => entry.topic === topicName)?.partitions[0]?.offset,
        });
      },
    });
    await join;
    await producer!.send({
      acks: 1,
      topic: topicName,
      messages: [{ key: 'k', value: 'v', partition: 0 }],
    });
    await waitForMessages(snapshots, { number: 1 });

    expect(snapshots[0]?.running).toBe(true);
    expect(snapshots[0]?.stale).toBe(false);
    expect(snapshots[0]?.uncommitted).toBe(1n);
  });

  it('commits only resolved offsets when eachBatchAutoResolve is false', async () => {
    consumer = createConsumer({
      cluster: createCluster(),
      groupId,
      maxWaitTimeInMs: 100,
      logger: newLogger(),
    });
    admin = createAdmin({ cluster: createCluster(), logger: newLogger() });
    await consumer.connect();
    await producer!.connect();
    await admin.connect();
    await consumer.subscribe({ topic: topicName, fromBeginning: true });

    const consumed: bigint[] = [];
    let resolvedFirst = false;
    const join = waitForConsumerToJoinGroup(consumer);
    await consumer.run({
      autoCommit: false,
      eachBatchAutoResolve: false,
      eachBatch: async ({ batch, resolveOffset, commitOffsetsIfNecessary, pause }) => {
        if (resolvedFirst) return;
        const first = batch.messages[0];
        if (first == null) return;
        consumed.push(first.offset);
        resolveOffset(first.offset);
        await commitOffsetsIfNecessary();
        resolvedFirst = true;
        pause();
      },
    });
    await join;
    await producer!.send({
      acks: 1,
      topic: topicName,
      messages: generateMessages({ number: 5 }).map((message) => ({ ...message, partition: 0 })),
    });
    await waitForMessages(consumed, { number: 1 });
    await consumer.disconnect();
    consumer = undefined;

    const offsets = await admin.fetchOffsets({ groupId, topics: [topicName] });
    const partition0 = offsets
      .find((entry) => entry.topic === topicName)
      ?.partitions.find((entry) => entry.partition === 0);
    expect(partition0?.offset).toBe(1n);

    consumer = createConsumer({
      cluster: createCluster(),
      groupId,
      maxWaitTimeInMs: 100,
      logger: newLogger(),
    });
    await consumer.connect();
    await consumer.subscribe({ topic: topicName });
    const remaining: bigint[] = [];
    const rejoin = waitForConsumerToJoinGroup(consumer);
    await consumer.run({
      eachMessage: async (event) => {
        if (event.partition === 0) remaining.push(event.message.offset);
      },
    });
    await rejoin;
    await waitForMessages(remaining, { number: 4 });
    expect(remaining).toEqual([1n, 2n, 3n, 4n]);
  });

  it('processes partitions concurrently when partitionsConsumedConcurrently is greater than 1', async () => {
    consumer = createConsumer({
      cluster: createCluster(),
      groupId,
      maxWaitTimeInMs: 50,
      logger: newLogger(),
    });
    await consumer.connect();
    await producer!.connect();
    await consumer.subscribe({ topic: topicName, fromBeginning: true });

    let inFlight = 0;
    let maxInFlight = 0;
    const consumed: { partition: number; offset: bigint }[] = [];
    const join = waitForConsumerToJoinGroup(consumer);
    await consumer.run({
      partitionsConsumedConcurrently: 2,
      eachBatch: async ({ batch }) => {
        inFlight += 1;
        maxInFlight = Math.max(maxInFlight, inFlight);
        await new Promise((resolve) => setTimeout(resolve, 200));
        for (const message of batch.messages) consumed.push({ partition: batch.partition, offset: message.offset });
        inFlight -= 1;
      },
    });
    await join;
    await producer!.send({
      acks: 1,
      topic: topicName,
      messages: [
        { key: 'a', value: 'a', partition: 0 },
        { key: 'b', value: 'b', partition: 1 },
      ],
    });
    await waitForMessages(consumed, { number: 2 });

    expect(new Set(consumed.map((entry) => entry.partition))).toEqual(new Set([0, 1]));
    expect(maxInFlight).toBeGreaterThan(1);
  });
});
