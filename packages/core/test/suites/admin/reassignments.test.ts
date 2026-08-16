import { afterEach, beforeEach, describe, expect } from 'vitest';
import { createAdmin } from '../../../src/admin/index';
import { createCluster, createTopic, newLogger, secureRandom, testIfKafkaAtLeast_2_4 } from '../../helpers/index';

describe('admin.reassignments', () => {
  let topicName: string;
  let admin: ReturnType<typeof createAdmin> | undefined;

  beforeEach(async () => {
    topicName = `test-topic-${secureRandom()}`;
    await createTopic({ topic: topicName, partitions: 1, replicas: 2 });
  });

  afterEach(async () => {
    await admin?.disconnect();
  });

  testIfKafkaAtLeast_2_4('lists partition reassignments (none in progress)', async () => {
    admin = createAdmin({ cluster: createCluster(), logger: newLogger() });
    await admin.connect();

    const listed = await admin.listPartitionReassignments({
      topics: [{ topic: topicName, partitions: [0] }],
    });
    expect(listed.topics).toEqual(expect.any(Array));
  });

  testIfKafkaAtLeast_2_4('alters partition reassignments', async () => {
    admin = createAdmin({ cluster: createCluster(), logger: newLogger() });
    await admin.connect();
    const { brokers } = await admin.describeCluster();
    const replicas = brokers.map((b) => b.nodeId);

    await admin.alterPartitionReassignments({
      topics: [{ topic: topicName, partitionAssignment: [{ partition: 0, replicas }] }],
    });

    const listed = await admin.listPartitionReassignments({
      topics: [{ topic: topicName, partitions: [0] }],
    });
    expect(listed.topics).toEqual(expect.any(Array));
  });
});
