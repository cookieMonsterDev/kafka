import { afterEach, expect } from 'vitest';
import { createAdmin } from '../../../src/admin/index';
import { createCluster, describeIfKRaft, newLogger, testIfKafkaAtLeast_3_6 } from '../../helpers/index';

describeIfKRaft('admin.metadata-quorum', () => {
  let admin: ReturnType<typeof createAdmin> | undefined;

  afterEach(async () => {
    await admin?.disconnect();
  });

  testIfKafkaAtLeast_3_6('returns metadata quorum state from the active controller', async () => {
    admin = createAdmin({ cluster: createCluster(), logger: newLogger() });
    await admin.connect();

    const quorum = await admin.describeMetadataQuorum();
    const metadataTopic = quorum.topics.find(({ topicName }) => topicName === '__cluster_metadata');
    expect(metadataTopic).toBeDefined();

    const partition = metadataTopic?.partitions.find(({ partitionIndex }) => partitionIndex === 0);
    expect(partition).toEqual(
      expect.objectContaining({
        errorCode: 0,
        leaderId: expect.any(Number),
        leaderEpoch: expect.any(Number),
        highWatermark: expect.any(BigInt),
        currentVoters: expect.arrayContaining([
          expect.objectContaining({
            replicaId: expect.any(Number),
            logEndOffset: expect.any(BigInt),
          }),
        ]),
      }),
    );
  });
});

describeIfKRaft('admin.unregister-broker', () => {
  let admin: ReturnType<typeof createAdmin> | undefined;

  afterEach(async () => {
    await admin?.disconnect();
  });

  testIfKafkaAtLeast_3_6('rejects unregistering an unknown broker id', async () => {
    admin = createAdmin({ cluster: createCluster(), logger: newLogger() });
    await admin.connect();

    await expect(admin.unregisterBroker({ brokerId: 99_999 })).rejects.toMatchObject({
      type: expect.stringMatching(/BROKER|NOT/i),
    });
  });
});

describeIfKRaft('admin.raft-voters', () => {
  let admin: ReturnType<typeof createAdmin> | undefined;

  afterEach(async () => {
    await admin?.disconnect();
  });

  testIfKafkaAtLeast_3_6('rejects adding a raft voter with a bogus directory id', async () => {
    admin = createAdmin({ cluster: createCluster(), logger: newLogger() });
    await admin.connect();

    await expect(
      admin.addRaftVoter({
        voterId: 99,
        voterDirectoryId: Buffer.alloc(16),
        listeners: [{ name: 'CONTROLLER', host: 'localhost', port: 9093 }],
      }),
    ).rejects.toMatchObject({
      type: expect.stringMatching(/VOTER|QUORUM|INVALID|UNKNOWN/i),
    });
  });

  testIfKafkaAtLeast_3_6('rejects removing a raft voter that is not in the quorum', async () => {
    admin = createAdmin({ cluster: createCluster(), logger: newLogger() });
    await admin.connect();

    await expect(
      admin.removeRaftVoter({
        voterId: 99,
        voterDirectoryId: Buffer.alloc(16),
      }),
    ).rejects.toMatchObject({
      type: expect.stringMatching(/VOTER|QUORUM|INVALID|UNKNOWN/i),
    });
  });
});
