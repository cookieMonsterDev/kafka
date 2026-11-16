import { describe, expect, it, vi } from 'vitest';
import type { Cluster } from '../cluster/index';
import { CONTROLLER } from '../cluster/run';
import { fakeRunCluster } from '../cluster/test-run';
import { createLogger, LOG_LEVELS } from '../loggers/index';
import { createMetadataQuorumApi } from './metadata-quorum';
import { withSend } from '../broker/test-send';

const silentLogger = createLogger({ level: LOG_LEVELS.NOTHING, logCreator: () => () => {} });

describe('admin/metadata-quorum', () => {
  it('targets the active controller for the metadata partition', async () => {
    const broker = withSend({
      describeQuorum: vi.fn().mockResolvedValue({
        errorCode: 0,
        topics: [
          {
            topicName: '__cluster_metadata',
            partitions: [
              {
                partitionIndex: 0,
                errorCode: 0,
                leaderId: 1,
                leaderEpoch: 2,
                highWatermark: 10n,
                currentVoters: [{ replicaId: 1, logEndOffset: 10n }],
                observers: [],
              },
            ],
          },
        ],
      }),
    });
    const cluster = fakeRunCluster({ controller: broker });
    const api = createMetadataQuorumApi({
      cluster: cluster as unknown as Cluster,
      logger: silentLogger.namespace('Admin'),
      rootLogger: silentLogger,
    });

    await expect(api.describeMetadataQuorum()).resolves.toEqual({
      topics: [
        {
          topicName: '__cluster_metadata',
          partitions: [
            {
              partitionIndex: 0,
              errorCode: 0,
              leaderId: 1,
              leaderEpoch: 2,
              highWatermark: 10n,
              currentVoters: [{ replicaId: 1, logEndOffset: 10n }],
              observers: [],
            },
          ],
        },
      ],
    });

    expect(cluster.resolveTarget).toHaveBeenCalledWith(CONTROLLER);
    expect(broker.describeQuorum).toHaveBeenCalledWith({
      topics: [{ topicName: '__cluster_metadata', partitions: [{ partitionIndex: 0 }] }],
    });
  });
});
