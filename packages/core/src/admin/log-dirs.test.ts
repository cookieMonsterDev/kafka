import { describe, expect, it, vi } from 'vitest';
import type { Cluster } from '../cluster/index';
import { KafkaNonRetriableError, KafkaProtocolError } from '../errors';
import { createLogger, LOG_LEVELS } from '../loggers/index';
import { createLogDirsApi } from './log-dirs';
import { withSend } from '../broker/test-send';
import { NOT_CONTROLLER_ERRORS, STALE_METADATA_ERRORS } from '../cluster/error-classifier';
import { nodeTarget } from '../cluster/run';
import { fakeRunCluster } from '../cluster/test-run';
import type { RetryOptions } from '../retry/index';

const silentLogger = createLogger({ level: LOG_LEVELS.NOTHING, logCreator: () => () => {} });

function makeApi(cluster: object, retry?: RetryOptions) {
  return createLogDirsApi({
    cluster: cluster as unknown as Cluster,
    logger: silentLogger.namespace('Admin'),
    rootLogger: silentLogger,
    retry,
  });
}

describe('admin/log-dirs', () => {
  describe('describeLogDirs', () => {
    it('throws when no brokers are available', async () => {
      const cluster = fakeRunCluster({}, { brokerPool: { brokers: {} } });
      await expect(makeApi(cluster).describeLogDirs()).rejects.toThrow('No brokers available to describe log dirs');
      expect(cluster.resolveTarget).not.toHaveBeenCalled();
    });

    it('queries every broker in the pool when brokerIds are omitted', async () => {
      const broker = withSend({ describeLogDirs: vi.fn().mockResolvedValue({ logDirs: [{ logDir: '/data' }] }) });
      const cluster = fakeRunCluster({ node: broker }, { brokerPool: { brokers: { '1': {}, '2': {} } } });
      await expect(makeApi(cluster).describeLogDirs({ topics: null })).resolves.toEqual({
        brokers: [
          { brokerId: 1, logDirs: [{ logDir: '/data' }] },
          { brokerId: 2, logDirs: [{ logDir: '/data' }] },
        ],
      });
      expect(cluster.refreshMetadata).toHaveBeenCalled();
      expect(cluster.resolveTarget).toHaveBeenCalledWith(nodeTarget('1'));
      expect(cluster.resolveTarget).toHaveBeenCalledWith(nodeTarget('2'));
      expect(cluster.run).toHaveBeenCalledWith(
        expect.any(Function),
        expect.any(Function),
        expect.objectContaining({
          refreshMetadata: true,
          retryOn: new Set([...NOT_CONTROLLER_ERRORS, ...STALE_METADATA_ERRORS]),
        }),
      );
    });

    it('queries only the requested broker ids', async () => {
      const broker = withSend({ describeLogDirs: vi.fn().mockResolvedValue({ logDirs: [] }) });
      const cluster = fakeRunCluster({ node: broker });
      await makeApi(cluster).describeLogDirs({ brokerIds: [3] });
      expect(cluster.resolveTarget).toHaveBeenCalledTimes(1);
      expect(cluster.resolveTarget).toHaveBeenCalledWith(nodeTarget('3'));
    });
  });

  describe('alterReplicaLogDirs', () => {
    it('rejects a missing or empty dirs list', async () => {
      const api = makeApi({});
      await expect(api.alterReplicaLogDirs({ dirs: undefined as never, brokerId: 1 })).rejects.toThrow(
        KafkaNonRetriableError,
      );
      await expect(api.alterReplicaLogDirs({ dirs: [], brokerId: 1 })).rejects.toThrow('Invalid replica log dir list');
    });

    it('alters replica log dirs on the requested broker', async () => {
      const dirs = [{ path: '/data', topics: [{ topic: 'orders', partitions: [0] }] }];
      const broker = withSend({ alterReplicaLogDirs: vi.fn().mockResolvedValue({ results: [{ topic: 'orders' }] }) });
      const cluster = fakeRunCluster({ node: broker });
      await expect(makeApi(cluster).alterReplicaLogDirs({ dirs, brokerId: 1 })).resolves.toEqual({
        results: [{ topic: 'orders' }],
      });
      expect(cluster.refreshMetadata).toHaveBeenCalled();
      expect(cluster.resolveTarget).toHaveBeenCalledWith(nodeTarget(1));
      expect(broker.alterReplicaLogDirs).toHaveBeenCalledWith({ dirs });
    });

    it('refreshes metadata and retries on NOT_LEADER_OR_FOLLOWER', async () => {
      const error = Object.assign(new Error('not leader'), { type: 'NOT_LEADER_OR_FOLLOWER' });
      const broker = withSend({ alterReplicaLogDirs: vi.fn().mockRejectedValue(error) });
      const cluster = fakeRunCluster({ node: broker });
      await expect(
        makeApi(cluster, { retries: 1, initialRetryTime: 1 }).alterReplicaLogDirs({
          dirs: [{ path: '/data', topics: [] }],
          brokerId: 1,
        }),
      ).rejects.toThrow();
      expect(broker.alterReplicaLogDirs).toHaveBeenCalledTimes(2);
      expect(cluster.refreshMetadata).toHaveBeenCalledTimes(2);
      expect(cluster.recover).toHaveBeenCalledWith(error);
    });
  });

  describe('describeReplicaLogDirs', () => {
    it('rejects an empty list and invalid replica fields', async () => {
      const api = makeApi({});
      await expect(api.describeReplicaLogDirs([])).rejects.toThrow('Invalid replica list');
      await expect(api.describeReplicaLogDirs([{ topic: '', partition: 0, brokerId: 1 }])).rejects.toThrow(
        'Invalid topic',
      );
      await expect(api.describeReplicaLogDirs([{ topic: 'orders', partition: -1, brokerId: 1 }])).rejects.toThrow(
        'Invalid partition',
      );
      await expect(
        api.describeReplicaLogDirs([{ topic: 'orders', partition: 0, brokerId: undefined as never }]),
      ).rejects.toThrow('Invalid brokerId');
    });

    it('maps a protocol error from one broker onto each of its replicas', async () => {
      const cluster = fakeRunCluster(
        {},
        {
          findBroker: vi
            .fn()
            .mockRejectedValue(
              new KafkaProtocolError({ type: 'BROKER_NOT_AVAILABLE', code: 8, message: 'The broker is not available' }),
            ),
        },
      );
      const result = await makeApi(cluster).describeReplicaLogDirs([
        { topic: 'orders', partition: 0, brokerId: 1 },
        { topic: 'orders', partition: 1, brokerId: 1 },
      ]);
      expect(result.replicas).toEqual([
        { topic: 'orders', partition: 0, brokerId: 1, logDir: null, errorCode: 8 },
        { topic: 'orders', partition: 1, brokerId: 1, logDir: null, errorCode: 8 },
      ]);
    });
  });
});
