import { describe, expect, it, vi } from 'vitest';
import type { Cluster } from '../cluster/index';
import { KafkaNonRetriableError } from '../errors';
import { createLogger, LOG_LEVELS } from '../loggers/index';
import { createReassignmentsApi } from './reassignments';

const silentLogger = createLogger({ level: LOG_LEVELS.NOTHING, logCreator: () => () => {} });

function makeApi(cluster: Record<string, unknown>, retry?: { retries?: number }) {
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
      const broker = { alterPartitionReassignments: vi.fn().mockResolvedValue(undefined) };
      const cluster = {
        refreshMetadata: vi.fn().mockResolvedValue(undefined),
        findControllerBroker: vi.fn().mockResolvedValue(broker),
      };
      await makeApi(cluster).alterPartitionReassignments({ topics: [validAssignment], timeout: 5000 });
      expect(broker.alterPartitionReassignments).toHaveBeenCalledWith({ topics: [validAssignment], timeout: 5000 });
    });

    it('retries on NOT_CONTROLLER', async () => {
      const error = Object.assign(new Error('not controller'), { type: 'NOT_CONTROLLER' });
      const cluster = {
        refreshMetadata: vi.fn().mockResolvedValue(undefined),
        findControllerBroker: vi.fn().mockResolvedValue({
          alterPartitionReassignments: vi.fn().mockRejectedValue(error),
        }),
      };
      await expect(
        makeApi(cluster, { retries: 0 }).alterPartitionReassignments({ topics: [validAssignment] }),
      ).rejects.toThrow();
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
      const broker = { listPartitionReassignments: vi.fn().mockResolvedValue({ topics: [] }) };
      const cluster = {
        refreshMetadata: vi.fn().mockResolvedValue(undefined),
        findControllerBroker: vi.fn().mockResolvedValue(broker),
      };
      await expect(makeApi(cluster).listPartitionReassignments()).resolves.toEqual({ topics: [] });
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
      const broker = { electLeaders: vi.fn().mockResolvedValue({ results: [{ topic: 'orders' }] }) };
      const cluster = {
        refreshMetadata: vi.fn().mockResolvedValue(undefined),
        findControllerBroker: vi.fn().mockResolvedValue(broker),
      };
      const topicPartitions = [{ topic: 'orders', partitions: [0] }];
      await expect(makeApi(cluster).electLeaders({ topicPartitions, electionType: 1, timeout: 1000 })).resolves.toEqual(
        {
          results: [{ topic: 'orders' }],
        },
      );
      expect(broker.electLeaders).toHaveBeenCalledWith({ topicPartitions, electionType: 1, timeout: 1000 });
    });
  });
});
