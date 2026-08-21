import { afterEach, beforeEach, describe, expect } from 'vitest';
import { createAdmin } from '../../../src/admin/index';
import { createCluster, newLogger, secureRandom, testIfKafkaAtLeast_4_0, waitFor } from '../../helpers/index';

describe('admin.describeTopicPartitions', () => {
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

  testIfKafkaAtLeast_4_0('describes partitions by name and returns topicId', async () => {
    admin = createAdmin({ cluster: createCluster(), logger: newLogger() });
    await admin.connect();
    await admin.createTopics({
      waitForLeaders: true,
      topics: [{ topic: topicName, numPartitions: 3, replicationFactor: 1 }],
    });

    const described = await waitFor(async () => {
      const page = await admin!.describeTopicPartitions({ topics: [topicName] });
      const topic = page.topics.find((entry) => entry.name === topicName);
      return topic?.partitions.length === 3 ? page : false;
    });

    const topic = described.topics.find((entry) => entry.name === topicName);
    expect(topic?.topicId).toEqual(expect.any(Buffer));
    expect(topic?.topicId).toHaveLength(16);
    expect(topic?.partitions.map((partition) => partition.partitionIndex).sort((a, b) => a - b)).toEqual([0, 1, 2]);
    expect(described.nextCursor).toBeNull();
  });

  testIfKafkaAtLeast_4_0('returns nextCursor when the response is truncated', async () => {
    admin = createAdmin({ cluster: createCluster(), logger: newLogger() });
    await admin.connect();
    await admin.createTopics({
      waitForLeaders: true,
      topics: [{ topic: topicName, numPartitions: 3, replicationFactor: 1 }],
    });

    const page = await waitFor(async () => {
      const result = await admin!.describeTopicPartitions({
        topics: [topicName],
        responsePartitionLimit: 1,
      });
      return result.topics[0]?.partitions.length === 1 ? result : false;
    });

    expect(page.nextCursor).toEqual({ topic: topicName, partitionIndex: 1 });

    const nextPage = await admin.describeTopicPartitions({
      topics: [topicName],
      responsePartitionLimit: 1,
      cursor: page.nextCursor,
    });
    expect(nextPage.topics[0]?.partitions[0]?.partitionIndex).toBe(1);
  });
});
