import { afterEach, describe, expect } from 'vitest';
import { createAdmin } from '../../../src/admin/index';
import { createCluster, newLogger, testIfKafkaAtLeast_1_1 } from '../../helpers/index';

describe('admin.logDirs', () => {
  let admin: ReturnType<typeof createAdmin> | undefined;

  afterEach(async () => {
    await admin?.disconnect();
  });

  testIfKafkaAtLeast_1_1('describes log dirs on every broker', async () => {
    admin = createAdmin({ cluster: createCluster(), logger: newLogger() });
    await admin.connect();

    const described = await admin.describeLogDirs();
    expect(described.brokers.length).toBeGreaterThan(0);
    expect(described.brokers[0]?.logDirs.length).toBeGreaterThan(0);
    expect(described.brokers[0]?.logDirs[0]).toEqual(
      expect.objectContaining({
        logDir: expect.any(String),
        errorCode: 0,
      }),
    );
  });

  testIfKafkaAtLeast_1_1('describes replica log dirs for a topic partition on a broker', async () => {
    admin = createAdmin({ cluster: createCluster(), logger: newLogger() });
    await admin.connect();

    const metadata = await admin.fetchTopicMetadata();
    const topic = metadata.topics[0];
    const partition = topic?.partitions[0];
    expect(topic?.name).toEqual(expect.any(String));
    expect(partition?.leader).toEqual(expect.any(Number));
    if (!topic || partition?.leader == null) {
      throw new Error('expected topic metadata with a partition leader');
    }

    const described = await admin.describeReplicaLogDirs([
      { topic: topic.name, partition: partition.partitionId, brokerId: partition.leader },
    ]);
    expect(described.replicas[0]?.errorCode ?? 0).toBe(0);
    expect(described.replicas[0]?.logDir).toEqual(expect.any(String));
  });
});
