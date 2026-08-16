import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { createAdmin } from '../../../src/admin/index.js';
import { createCluster, newLogger, secureRandom, waitFor } from '../../helpers/index.js';

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

  it('adds partitions to an existing topic', async () => {
    admin = createAdmin({ cluster: createCluster(), logger: newLogger() });
    await admin.connect();
    await admin.createTopics({ waitForLeaders: true, topics: [{ topic: topicName, numPartitions: 1 }] });

    await admin.createPartitions({ topicPartitions: [{ topic: topicName, count: 3 }] });

    await waitFor(async () => {
      const metadata = await admin!.fetchTopicMetadata({ topics: [topicName] });
      const topic = metadata.topics.find((t) => t.name === topicName);
      return topic?.partitions.length === 3 ? topic : false;
    });

    const metadata = await admin.fetchTopicMetadata({ topics: [topicName] });
    const topic = metadata.topics.find((t) => t.name === topicName);
    expect(topic?.partitions).toHaveLength(3);
  });

  it('rejects creating partitions for a missing topic', async () => {
    admin = createAdmin({ cluster: createCluster(), logger: newLogger() });
    await admin.connect();
    await expect(
      admin.createPartitions({ topicPartitions: [{ topic: `${topicName}-missing`, count: 3 }] }),
    ).rejects.toThrow(/This server does not host this topic-partition/);
  });
});
