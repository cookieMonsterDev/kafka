import { describe, expect, it, vi } from 'vitest';
import type { Cluster } from '../cluster/index';
import { createLogger, LOG_LEVELS } from '../loggers/index';
import { createLogDirsApi } from './log-dirs';
import { withSend } from '../broker/test-send';
import { fakeRunCluster } from '../cluster/test-run';

const logger = createLogger({ level: LOG_LEVELS.NOTHING, logCreator: () => () => {} });

describe('admin/describeReplicaLogDirs', () => {
  it('queries each broker with a filtered topic list and maps partitions back to replicas', async () => {
    const broker = withSend({
      describeLogDirs: vi.fn(async () => ({
        logDirs: [
          {
            errorCode: 0,
            logDir: '/var/kafka/data',
            topics: [
              {
                topic: 'orders',
                partitions: [{ partition: 0, size: 100n, offsetLag: 0n, isFuture: false }],
              },
            ],
          },
        ],
      })),
    });
    const cluster = fakeRunCluster({}, { findBroker: vi.fn(async () => broker) });
    const api = createLogDirsApi({ cluster: cluster as unknown as Cluster, logger, rootLogger: logger });

    const result = await api.describeReplicaLogDirs([{ topic: 'orders', partition: 0, brokerId: 1 }]);

    expect(cluster.refreshMetadata).toHaveBeenCalled();
    expect(cluster.findBroker).toHaveBeenCalledWith({ nodeId: '1' });
    expect(broker.describeLogDirs).toHaveBeenCalledWith({
      topics: [{ topic: 'orders', partitions: [0] }],
    });
    expect(result.replicas[0]).toEqual({
      topic: 'orders',
      partition: 0,
      brokerId: 1,
      logDir: '/var/kafka/data',
      errorCode: 0,
      size: 100n,
      offsetLag: 0n,
      isFuture: false,
    });
  });
});
