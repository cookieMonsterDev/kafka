import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { createAdmin } from '../../../src/admin/index';
import { createProducer } from '../../../src/producer/index';
import {
  createCluster,
  generateMessages,
  newLogger,
  secureRandom,
  testIfKafkaAtLeast_0_11,
  waitFor,
} from '../../helpers/index';

describe('admin.topics', () => {
  let topicName: string;
  let admin: ReturnType<typeof createAdmin> | undefined;
  let producer: ReturnType<typeof createProducer> | undefined;

  beforeEach(() => {
    topicName = `test-topic-${secureRandom()}`;
  });

  afterEach(async () => {
    await producer?.disconnect();
    if (admin) {
      await admin.deleteTopics({ topics: [topicName] }).catch(() => undefined);
      await admin.disconnect();
    }
  });

  it('creates, lists, describes, and deletes a topic', async () => {
    admin = createAdmin({ cluster: createCluster(), logger: newLogger() });
    await admin.connect();

    await expect(
      admin.createTopics({
        waitForLeaders: true,
        topics: [{ topic: topicName, numPartitions: 1, replicationFactor: 1 }],
      }),
    ).resolves.toBe(true);
    await expect(
      admin.createTopics({
        waitForLeaders: true,
        topics: [{ topic: topicName, numPartitions: 1, replicationFactor: 1 }],
      }),
    ).resolves.toBe(false);

    expect(await admin.listTopics()).toEqual(expect.arrayContaining([topicName]));

    const metadata = await admin.fetchTopicMetadata({ topics: [topicName] });
    expect(metadata.topics).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          name: topicName,
          partitions: expect.arrayContaining([expect.objectContaining({ partitionId: 0 })]),
        }),
      ]),
    );

    const cluster = await admin.describeCluster();
    expect(cluster.brokers).toHaveLength(3);
    expect(cluster.clusterId).toEqual(expect.any(String));
    expect(cluster.brokers.map((b) => b.nodeId)).toContain(cluster.controller);

    await admin.deleteTopics({ topics: [topicName] });
    const remaining = await waitFor(async () => {
      const topics = await admin!.listTopics();
      return topics.includes(topicName) ? false : topics;
    });
    expect(remaining).not.toEqual(expect.arrayContaining([topicName]));
  });

  it('creates a topic with manual replica assignment', async () => {
    admin = createAdmin({ cluster: createCluster(), logger: newLogger() });
    await admin.connect();
    const { brokers } = await admin.describeCluster();
    const replicas = brokers.slice(0, 2).map((b) => b.nodeId);

    await expect(
      admin.createTopics({
        waitForLeaders: true,
        topics: [{ topic: topicName, replicaAssignment: [{ partition: 0, replicas }] }],
      }),
    ).resolves.toBe(true);
  });

  testIfKafkaAtLeast_0_11('deletes records up to an offset', async () => {
    admin = createAdmin({ cluster: createCluster(), logger: newLogger() });
    producer = createProducer({ cluster: createCluster(), logger: newLogger() });
    await admin.connect();
    await producer.connect();
    await admin.createTopics({
      waitForLeaders: true,
      topics: [{ topic: topicName, numPartitions: 1, replicationFactor: 1 }],
    });
    await producer.send({ acks: 1, topic: topicName, messages: generateMessages({ number: 10 }) });

    await admin.deleteTopicRecords({ topic: topicName, partitions: [{ partition: 0, offset: 5n }] });

    const offsets = await waitFor(async () => {
      const topicOffsets = await admin!.fetchTopicOffsets(topicName);
      const partition = topicOffsets.find((entry) => entry.partition === 0);
      return partition?.low === 5n ? partition : false;
    });
    expect(offsets.high).toBe(10n);
    expect(offsets.low).toBe(5n);
  });
});
