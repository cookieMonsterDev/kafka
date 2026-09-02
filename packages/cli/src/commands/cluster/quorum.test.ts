import { describe, expect, it } from 'vitest';
import { createFakeAdmin } from '../../testing/create-fake-admin';
import { createFakeCommandContext } from '../../testing/create-command-context';
import { clusterQuorumCommand } from './quorum';

function fakeQuorum() {
  return {
    topics: [
      {
        topicName: '__cluster_metadata',
        partitions: [
          {
            partitionIndex: 0,
            errorCode: 0,
            leaderId: 1,
            leaderEpoch: 3,
            highWatermark: 42n,
            currentVoters: [{ replicaId: 1, logEndOffset: 42n }],
            observers: [{ replicaId: 2, logEndOffset: 40n }],
          },
        ],
      },
    ],
  };
}

describe('clusterQuorumCommand', () => {
  it('renders one row per partition with voters and observers', async () => {
    const describeMetadataQuorum = async () => fakeQuorum();
    const admin = createFakeAdmin({ describeMetadataQuorum, disconnect: async () => {} });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      openAdmin: async () => admin,
    });

    const code = await clusterQuorumCommand.run(context);

    expect(code).toBe(0);
    const written = stdoutWrite.mock.calls[0]![0];
    expect(written).toContain('__cluster_metadata');
    expect(written).toContain('42');
    expect(written).toContain('1');
    expect(written).toContain('2');
  });

  it('renders a placeholder when there are no topics', async () => {
    const describeMetadataQuorum = async () => ({ topics: [] });
    const admin = createFakeAdmin({ describeMetadataQuorum, disconnect: async () => {} });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      openAdmin: async () => admin,
    });

    await clusterQuorumCommand.run(context);

    expect(stdoutWrite.mock.calls[0]![0]).toContain('(no metadata quorum topics)');
  });

  it('reports json output with bigints encoded as strings', async () => {
    const describeMetadataQuorum = async () => fakeQuorum();
    const admin = createFakeAdmin({ describeMetadataQuorum, disconnect: async () => {} });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      openAdmin: async () => admin,
      format: 'json',
    });

    await clusterQuorumCommand.run(context);

    const written = JSON.parse(stdoutWrite.mock.calls[0]![0]) as {
      topics: { partitions: { highWatermark: string }[] }[];
    };
    expect(written.topics[0]?.partitions[0]?.highWatermark).toBe('42');
  });
});
