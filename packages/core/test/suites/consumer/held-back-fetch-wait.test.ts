import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { createAdmin } from '../../../src/admin/index';
import { createConsumer } from '../../../src/consumer/index';
import { createProducer } from '../../../src/producer/index';
import { createCluster, generateMessages, newLogger, secureRandom, waitFor } from '../../helpers/index';

// Large enough that one stalled long poll alone fails the time bound below, which still leaves
// seconds of headroom for a loaded CI broker.
const MAX_WAIT_TIME_MS = 10_000;
const BUSY_PARTITION_SENDS = 20;
const MESSAGES_PER_SEND = 10;
const BATCH_HANDLING_MS = 30;

describe('consumer fetch while a partition is held back', () => {
  let topicName: string;
  let producer: ReturnType<typeof createProducer> | undefined;
  let consumer: ReturnType<typeof createConsumer> | undefined;

  beforeEach(async () => {
    topicName = `test-topic-${secureRandom()}`;
    // Both partitions on one broker, so they share that node's single in-flight fetch.
    const admin = createAdmin({ cluster: createCluster(), logger: newLogger() });
    try {
      await admin.connect();
      const { brokers } = await admin.describeCluster();
      const nodeId = brokers[0]!.nodeId;
      await admin.createTopics({
        waitForLeaders: true,
        topics: [
          {
            topic: topicName,
            replicaAssignment: [
              { partition: 0, replicas: [nodeId] },
              { partition: 1, replicas: [nodeId] },
            ],
          },
        ],
      });
    } finally {
      await admin.disconnect();
    }
    producer = createProducer({ cluster: createCluster(), logger: newLogger() });
  });

  afterEach(async () => {
    await consumer?.disconnect();
    await producer?.disconnect();
  });

  it('does not stall a busy partition behind a long poll on a drained one', async () => {
    await producer!.connect();
    await producer!.send({ acks: 1, topic: topicName, messages: [{ partition: 0, value: 'only' }] });
    // One send per record batch, so the small maxBytesPerPartition spreads them over many fetches.
    for (let index = 0; index < BUSY_PARTITION_SENDS; index++) {
      const messages = generateMessages({ number: MESSAGES_PER_SEND }).map((message) => ({ ...message, partition: 1 }));
      await producer!.send({ acks: 1, topic: topicName, messages });
    }

    consumer = createConsumer({
      cluster: createCluster(),
      groupId: `group-${secureRandom()}`,
      maxWaitTimeInMs: MAX_WAIT_TIME_MS,
      maxBytesPerPartition: 1024,
      logger: newLogger(),
    });
    await consumer.connect();
    await consumer.subscribe({ topic: topicName, fromBeginning: true });

    const offsetsByPartition = new Map<number, bigint[]>();
    let consumed = 0;
    const total = 1 + BUSY_PARTITION_SENDS * MESSAGES_PER_SEND;
    let firstBatchAt: number | undefined;
    await consumer.run({
      eachBatch: async ({ batch }) => {
        firstBatchAt ??= Date.now();
        const offsets = offsetsByPartition.get(batch.partition) ?? [];
        offsets.push(...batch.messages.map(({ offset }) => offset));
        offsetsByPartition.set(batch.partition, offsets);
        consumed += batch.messages.length;
        // Slower than a fetch round trip, so partition 1 reaches its prefetch limit.
        await new Promise((resolve) => setTimeout(resolve, BATCH_HANDLING_MS));
      },
    });

    await waitFor(() => consumed >= total, { delay: 20, maxWait: 20_000 });
    const elapsedMs = Date.now() - (firstBatchAt ?? Date.now());

    expect(consumed).toBe(total);
    expect(offsetsByPartition.get(0)).toEqual([0n]);
    expect(offsetsByPartition.get(1)).toEqual(
      Array.from({ length: BUSY_PARTITION_SENDS * MESSAGES_PER_SEND }, (_, index) => BigInt(index)),
    );
    expect(elapsedMs).toBeLessThan(MAX_WAIT_TIME_MS / 2);
  });
});
