import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { createAdmin } from '../../src/admin/index.js';
import { createCluster, createTopic, newLogger, secureRandom } from '../helpers/index.js';

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

  it('lists partition reassignments (none in progress)', async () => {
    admin = createAdmin({ cluster: createCluster(), logger: newLogger() });
    await admin.connect();

    const listed = await admin.listPartitionReassignments({
      topics: [{ topic: topicName, partitions: [0] }],
    });
    expect(listed.topics).toEqual(expect.any(Array));
  });

  it('alters partition reassignments', async () => {
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
