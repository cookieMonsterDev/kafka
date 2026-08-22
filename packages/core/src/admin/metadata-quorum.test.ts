import { describe, expect, it, vi } from 'vitest';
import type { Cluster } from '../cluster/index';
import { createLogger, LOG_LEVELS } from '../loggers/index';
import { createMetadataQuorumApi } from './metadata-quorum';

const silentLogger = createLogger({ level: LOG_LEVELS.NOTHING, logCreator: () => () => {} });

describe('admin/metadata-quorum', () => {
  it('targets the active controller for the metadata partition', async () => {
    const broker = {
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
    };
    const findControllerBroker = vi.fn().mockResolvedValue(broker);
    const cluster = {
      refreshMetadata: vi.fn().mockResolvedValue(undefined),
      findControllerBroker,
    } as unknown as Cluster;
    const api = createMetadataQuorumApi({
      cluster,
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

    expect(findControllerBroker).toHaveBeenCalled();
    expect(broker.describeQuorum).toHaveBeenCalledWith({
      topics: [{ topicName: '__cluster_metadata', partitions: [{ partitionIndex: 0 }] }],
    });
  });
});
