import { describe, expect, it, vi } from 'vitest';
import type { Cluster } from '../cluster/index';
import { KafkaNonRetriableError, KafkaProtocolError } from '../errors';
import { createLogger, LOG_LEVELS } from '../loggers/index';
import { createLogDirsApi } from './log-dirs';

const silentLogger = createLogger({ level: LOG_LEVELS.NOTHING, logCreator: () => () => {} });

function makeApi(cluster: Record<string, unknown>, retry?: { retries?: number }) {
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
      const cluster = {
        refreshMetadata: vi.fn().mockResolvedValue(undefined),
        brokerPool: { brokers: {} },
      };
      await expect(makeApi(cluster).describeLogDirs()).rejects.toThrow('No brokers available to describe log dirs');
    });

    it('queries every broker in the pool when brokerIds are omitted', async () => {
      const broker = { describeLogDirs: vi.fn().mockResolvedValue({ logDirs: [{ logDir: '/data' }] }) };
      const cluster = {
        refreshMetadata: vi.fn().mockResolvedValue(undefined),
        brokerPool: { brokers: { '1': {}, '2': {} } },
        findBroker: vi.fn().mockResolvedValue(broker),
      };
      await expect(makeApi(cluster).describeLogDirs({ topics: null })).resolves.toEqual({
        brokers: [
          { brokerId: 1, logDirs: [{ logDir: '/data' }] },
          { brokerId: 2, logDirs: [{ logDir: '/data' }] },
        ],
      });
      expect(cluster.findBroker).toHaveBeenCalledWith({ nodeId: '1' });
      expect(cluster.findBroker).toHaveBeenCalledWith({ nodeId: '2' });
    });

    it('queries only the requested broker ids', async () => {
      const broker = { describeLogDirs: vi.fn().mockResolvedValue({ logDirs: [] }) };
      const cluster = {
        refreshMetadata: vi.fn().mockResolvedValue(undefined),
        findBroker: vi.fn().mockResolvedValue(broker),
      };
      await makeApi(cluster).describeLogDirs({ brokerIds: [3] });
      expect(cluster.findBroker).toHaveBeenCalledTimes(1);
      expect(cluster.findBroker).toHaveBeenCalledWith({ nodeId: '3' });
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
      const dirs = [{ logDir: '/data', topics: [{ topic: 'orders', partitions: [0] }] }];
      const broker = { alterReplicaLogDirs: vi.fn().mockResolvedValue({ results: [{ topic: 'orders' }] }) };
      const cluster = {
        refreshMetadata: vi.fn().mockResolvedValue(undefined),
        findBroker: vi.fn().mockResolvedValue(broker),
      };
      await expect(makeApi(cluster).alterReplicaLogDirs({ dirs, brokerId: 1 })).resolves.toEqual({
        results: [{ topic: 'orders' }],
      });
      expect(cluster.findBroker).toHaveBeenCalledWith({ nodeId: '1' });
      expect(broker.alterReplicaLogDirs).toHaveBeenCalledWith({ dirs });
    });

    it('refreshes metadata and retries on NOT_LEADER_OR_FOLLOWER', async () => {
      const error = Object.assign(new Error('not leader'), { type: 'NOT_LEADER_OR_FOLLOWER' });
      const cluster = {
        refreshMetadata: vi.fn().mockResolvedValue(undefined),
        findBroker: vi.fn().mockResolvedValue({ alterReplicaLogDirs: vi.fn().mockRejectedValue(error) }),
      };
      await expect(
        makeApi(cluster, { retries: 0 }).alterReplicaLogDirs({
          dirs: [{ logDir: '/data', topics: [] }],
          brokerId: 1,
        }),
      ).rejects.toThrow();
      expect(cluster.refreshMetadata).toHaveBeenCalledTimes(2);
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
      const cluster = {
        refreshMetadata: vi.fn().mockResolvedValue(undefined),
        findBroker: vi.fn().mockRejectedValue(new KafkaProtocolError({ type: 'BROKER_NOT_AVAILABLE', code: 8 })),
      };
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
