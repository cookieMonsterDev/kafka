import { describe, expect, it, vi } from 'vitest';
import type { Cluster } from '../cluster/index';
import { CONTROLLER } from '../cluster/run';
import { fakeRunCluster } from '../cluster/test-run';
import { KafkaNonRetriableError } from '../errors';
import { createLogger, LOG_LEVELS } from '../loggers/index';
import { createReassignmentsApi } from './reassignments';
import { withSend } from '../broker/test-send';

const silentLogger = createLogger({ level: LOG_LEVELS.NOTHING, logCreator: () => () => {} });

function makeApi(cluster: object, retry?: { retries?: number }) {
  return createReassignmentsApi({
    cluster: cluster as unknown as Cluster,
    logger: silentLogger.namespace('Admin'),
    rootLogger: silentLogger,
    retry,
  });
}

const validAssignment = {
  topic: 'orders',
  partitionAssignment: [{ partition: 0, replicas: [1, 2] }],
};

describe('admin/reassignments', () => {
  describe('alterPartitionReassignments', () => {
    it('rejects a non-array, non-string names, duplicates, and bad partition/replica values', async () => {
      const api = makeApi({});
      await expect(api.alterPartitionReassignments({ topics: undefined as never })).rejects.toThrow(
        KafkaNonRetriableError,
      );
      await expect(
        api.alterPartitionReassignments({ topics: [{ topic: 1 as never, partitionAssignment: [] }] }),
      ).rejects.toThrow('the topic names have to be a valid string');
      await expect(
        api.alterPartitionReassignments({
          topics: [
            { topic: 'orders', partitionAssignment: [] },
            { topic: 'orders', partitionAssignment: [] },
          ],
        }),
      ).rejects.toThrow('cannot have multiple entries for the same topic');
      await expect(
        api.alterPartitionReassignments({ topics: [{ topic: 'orders', partitionAssignment: undefined as never }] }),
      ).rejects.toThrow('Invalid partitions array');
      await expect(
        api.alterPartitionReassignments({
          topics: [{ topic: 'orders', partitionAssignment: [{ partition: -1, replicas: [1] }] }],
        }),
      ).rejects.toThrow('Invalid partitions index');
      await expect(
        api.alterPartitionReassignments({
          topics: [{ topic: 'orders', partitionAssignment: [{ partition: 0, replicas: undefined as never }] }],
        }),
      ).rejects.toThrow('Invalid replica assignment');
      await expect(
        api.alterPartitionReassignments({
          topics: [{ topic: 'orders', partitionAssignment: [{ partition: 0, replicas: [-1] }] }],
        }),
      ).rejects.toThrow('Replicas must be a non negative number');
    });

    it('sends the assignment to the controller', async () => {
      const broker = withSend({ alterPartitionReassignments: vi.fn().mockResolvedValue(undefined) });
      const cluster = fakeRunCluster({ controller: broker });
      await makeApi(cluster).alterPartitionReassignments({ topics: [validAssignment], timeout: 5000 });
      expect(cluster.refreshMetadata).toHaveBeenCalled();
      expect(cluster.resolveTarget).toHaveBeenCalledWith(CONTROLLER);
      expect(broker.alterPartitionReassignments).toHaveBeenCalledWith({ topics: [validAssignment], timeout: 5000 });
    });

    it('passes allowReplicationFactorChange to the request', async () => {
      const broker = withSend({ alterPartitionReassignments: vi.fn().mockResolvedValue(undefined) });
      const cluster = fakeRunCluster({ controller: broker });
      await makeApi(cluster).alterPartitionReassignments({
        topics: [validAssignment],
        allowReplicationFactorChange: false,
      });

      expect(broker.alterPartitionReassignments).toHaveBeenCalledWith({
        topics: [validAssignment],
        allowReplicationFactorChange: false,
      });
    });

    it('retries on NOT_CONTROLLER', async () => {
      const error = Object.assign(new Error('not controller'), { type: 'NOT_CONTROLLER' });
      const broker = withSend({
        alterPartitionReassignments: vi.fn().mockRejectedValueOnce(error).mockResolvedValue(undefined),
      });
      const cluster = fakeRunCluster({ controller: broker });
      await expect(
        makeApi(cluster, { retries: 1 }).alterPartitionReassignments({ topics: [validAssignment] }),
      ).resolves.toBeUndefined();
      expect(broker.alterPartitionReassignments).toHaveBeenCalledTimes(2);
      expect(cluster.refreshMetadata).toHaveBeenCalledTimes(2);
      expect(cluster.recover).toHaveBeenCalledWith(error);
    });

    it('rejects a non-retriable broker error as is', async () => {
      const error = Object.assign(new Error('denied'), { type: 'CLUSTER_AUTHORIZATION_FAILED' });
      const broker = withSend({ alterPartitionReassignments: vi.fn().mockRejectedValue(error) });
      const cluster = fakeRunCluster({ controller: broker });
      await expect(makeApi(cluster).alterPartitionReassignments({ topics: [validAssignment] })).rejects.toBe(error);
      expect(broker.alterPartitionReassignments).toHaveBeenCalledTimes(1);
      expect(cluster.recover).not.toHaveBeenCalled();
    });
  });

  describe('listPartitionReassignments', () => {
    it('rejects invalid topic filters', async () => {
      const api = makeApi({});
      await expect(api.listPartitionReassignments({ topics: 'orders' as never })).rejects.toThrow(
        'Invalid topics array',
      );
      await expect(
        api.listPartitionReassignments({ topics: [{ topic: 1 as never, partitions: [0] }] }),
      ).rejects.toThrow('the topic names have to be a valid string');
      await expect(
        api.listPartitionReassignments({
          topics: [
            { topic: 'orders', partitions: [0] },
            { topic: 'orders', partitions: [1] },
          ],
        }),
      ).rejects.toThrow('cannot have multiple entries for the same topic');
      await expect(
        api.listPartitionReassignments({ topics: [{ topic: 'orders', partitions: undefined as never }] }),
      ).rejects.toThrow('Invalid partition array');
      await expect(api.listPartitionReassignments({ topics: [{ topic: 'orders', partitions: [-1] }] })).rejects.toThrow(
        'valid number greater than 0',
      );
    });

    it('lists reassignments for all topics when the filter is omitted', async () => {
      const broker = withSend({ listPartitionReassignments: vi.fn().mockResolvedValue({ topics: [] }) });
      const cluster = fakeRunCluster({ controller: broker });
      await expect(makeApi(cluster).listPartitionReassignments()).resolves.toEqual({ topics: [] });
      expect(cluster.resolveTarget).toHaveBeenCalledWith(CONTROLLER);
      expect(broker.listPartitionReassignments).toHaveBeenCalledWith({ topics: null, timeout: undefined });
    });
  });

  describe('electLeaders', () => {
    it('rejects invalid topicPartitions filters', async () => {
      const api = makeApi({});
      await expect(api.electLeaders({ topicPartitions: 'orders' as never })).rejects.toThrow(
        'Invalid topicPartitions array',
      );
      await expect(api.electLeaders({ topicPartitions: [{ topic: 1 as never, partitions: [0] }] })).rejects.toThrow(
        'the topic names have to be a valid string',
      );
      await expect(
        api.electLeaders({
          topicPartitions: [
            { topic: 'orders', partitions: [0] },
            { topic: 'orders', partitions: [1] },
          ],
        }),
      ).rejects.toThrow('cannot have multiple entries for the same topic');
      await expect(api.electLeaders({ topicPartitions: [{ topic: 'orders', partitions: [-1] }] })).rejects.toThrow(
        'valid number greater than 0',
      );
    });

    it('elects leaders through the controller', async () => {
      const broker = withSend({ electLeaders: vi.fn().mockResolvedValue({ results: [{ topic: 'orders' }] }) });
      const cluster = fakeRunCluster({ controller: broker });
      const topicPartitions = [{ topic: 'orders', partitions: [0] }];
      await expect(makeApi(cluster).electLeaders({ topicPartitions, electionType: 1, timeout: 1000 })).resolves.toEqual(
        {
          results: [{ topic: 'orders' }],
        },
      );
      expect(cluster.resolveTarget).toHaveBeenCalledWith(CONTROLLER);
      expect(broker.electLeaders).toHaveBeenCalledWith({ topicPartitions, electionType: 1, timeout: 1000 });
    });
  });
});
