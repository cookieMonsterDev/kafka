import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { createAdmin } from '../../../src/admin/index';
import { createCluster, newLogger, secureRandom, waitFor } from '../../helpers/index';

describe('admin.topics', () => {
  let topicName: string;
  let admin: ReturnType<typeof createAdmin> | undefined;

  beforeEach(() => {
    topicName = `test-topic-${secureRandom()}`;
  });

  afterEach(async () => {
    if (admin) {
      await admin.deleteTopics({ topics: [topicName] }).catch(() => undefined);
      await admin.disconnect();
    }
  });

  it('creates, lists, describes, and deletes a topic', async () => {
    admin = createAdmin({ cluster: createCluster(), logger: newLogger() });
    await admin.connect();

    await expect(admin.createTopics({ waitForLeaders: true, topics: [{ topic: topicName }] })).resolves.toBe(true);
    await expect(admin.createTopics({ waitForLeaders: true, topics: [{ topic: topicName }] })).resolves.toBe(false);

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
    await waitFor(async () => {
      const topics = await admin!.listTopics();
      return topics.includes(topicName) ? false : true;
    });
    expect(await admin.listTopics()).not.toEqual(expect.arrayContaining([topicName]));
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
});
