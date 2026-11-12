import { afterEach, beforeEach, describe, expect } from 'vitest';
import { createAdmin } from '../../../src/admin/index';
import { KafkaAggregateError } from '../../../src/errors';
import {
  createCluster,
  newLogger,
  secureRandom,
  testIfKafkaAtLeast_4_0,
  TRANSIENT_METADATA_ERRORS,
  waitFor,
} from '../../helpers/index';

type Admin = ReturnType<typeof createAdmin>;

const transientMetadataErrors = new Set<string>(TRANSIENT_METADATA_ERRORS);

/**
 * `createTopics({ waitForLeaders })` does not wait for every broker to learn a new topic, so the
 * broker that answers `DescribeTopicPartitions` can still report `UNKNOWN_TOPIC_OR_PARTITION` (seen
 * on Node.js and Bun alike against Kafka 4.0). Resolves `undefined` ("poll again") for that kind of
 * error and rethrows any other.
 */
async function describeWhenKnown(
  admin: Admin,
  options: Parameters<Admin['describeTopicPartitions']>[0],
): Promise<Awaited<ReturnType<Admin['describeTopicPartitions']>> | undefined> {
  try {
    return await admin.describeTopicPartitions(options);
  } catch (error) {
    const isMetadataLag =
      error instanceof KafkaAggregateError &&
      error.errors.every((inner) => transientMetadataErrors.has((inner as { type?: string }).type ?? ''));
    if (isMetadataLag) return undefined;
    throw error;
  }
}

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
      const page = await describeWhenKnown(admin!, { topics: [topicName] });
      const topic = page?.topics.find((entry) => entry.name === topicName);
      return page != null && topic?.partitions.length === 3 ? page : false;
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
      const result = await describeWhenKnown(admin!, { topics: [topicName], responsePartitionLimit: 1 });
      return result != null && result.topics[0]?.partitions.length === 1 ? result : false;
    });

    expect(page.nextCursor).toEqual({ topic: topicName, partitionIndex: 1 });

    const nextPage = await waitFor(async () => {
      const result = await describeWhenKnown(admin!, {
        topics: [topicName],
        responsePartitionLimit: 1,
        cursor: page.nextCursor,
      });
      return result ?? false;
    });
    expect(nextPage.topics[0]?.partitions[0]?.partitionIndex).toBe(1);
  });
});
