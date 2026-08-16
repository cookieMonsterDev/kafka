import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { createAdmin } from '../../../src/admin/index';
import { createCluster, newLogger, secureRandom, testIfKafkaAtLeast_1_0, waitFor } from '../../helpers/index';

describe('admin.partitions', () => {
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

  testIfKafkaAtLeast_1_0('adds partitions to an existing topic', async () => {
    admin = createAdmin({ cluster: createCluster(), logger: newLogger() });
    await admin.connect();
    await admin.createTopics({
      waitForLeaders: true,
      topics: [{ topic: topicName, numPartitions: 1, replicationFactor: 1 }],
    });

    await admin.createPartitions({ topicPartitions: [{ topic: topicName, count: 3 }] });

    const topic = await waitFor(async () => {
      const metadata = await admin!.fetchTopicMetadata({ topics: [topicName] });
      const found = metadata.topics.find((t) => t.name === topicName);
      return found?.partitions.length === 3 ? found : false;
    });
    expect(topic.partitions).toHaveLength(3);
  });

  testIfKafkaAtLeast_1_0('rejects creating partitions for a missing topic', async () => {
    admin = createAdmin({ cluster: createCluster(), logger: newLogger() });
    await admin.connect();
    await expect(
      admin.createPartitions({ topicPartitions: [{ topic: `${topicName}-missing`, count: 3 }] }),
    ).rejects.toThrow(/This server does not host this topic-partition/);
  });
});
