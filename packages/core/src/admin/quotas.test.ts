import { describe, expect, it, vi } from 'vitest';
import type { Cluster } from '../cluster/index';
import { KafkaNonRetriableError } from '../errors';
import { createLogger, LOG_LEVELS } from '../loggers/index';
import { createQuotasApi } from './quotas';
import type { AlterClientQuotasEntry } from '../protocol/requests/alter-client-quotas/index';
import type { DescribeClientQuotasComponent } from '../protocol/requests/describe-client-quotas/index';

const silentLogger = createLogger({ level: LOG_LEVELS.NOTHING, logCreator: () => () => {} });

function fakeCluster(broker: Record<string, ReturnType<typeof vi.fn>> = {}) {
  return {
    refreshMetadata: vi.fn().mockResolvedValue(undefined),
    findControllerBroker: vi.fn().mockResolvedValue(broker),
    broker,
  };
}

function makeApi(cluster: ReturnType<typeof fakeCluster>, retry?: { retries?: number }) {
  return createQuotasApi({
    cluster: cluster as unknown as Cluster,
    logger: silentLogger.namespace('Admin'),
    rootLogger: silentLogger,
    retry,
  });
}

const validEntry: AlterClientQuotasEntry = {
  entity: [{ entityType: 'client-id', entityName: 'app-1' }],
  ops: [{ key: 'producer_byte_rate', value: 1024, remove: false }],
};

describe('admin/quotas', () => {
  describe('describeClientQuotas', () => {
    it('rejects a non-array components option', async () => {
      const api = makeApi(fakeCluster());
      await expect(
        api.describeClientQuotas({
          components: 'not-an-array' as unknown as DescribeClientQuotasComponent[],
        }),
      ).rejects.toThrow(KafkaNonRetriableError);
    });

    it('describes quotas through the active controller with defaults', async () => {
      const cluster = fakeCluster({
        describeClientQuotas: vi.fn().mockResolvedValue({ entries: [{ entity: [], values: [] }] }),
      });
      const api = makeApi(cluster);

      await expect(api.describeClientQuotas()).resolves.toEqual({ entries: [{ entity: [], values: [] }] });

      expect(cluster.refreshMetadata).toHaveBeenCalled();
      expect(cluster.findControllerBroker).toHaveBeenCalled();
      expect(cluster.broker.describeClientQuotas).toHaveBeenCalledWith({ components: [], strict: false });
    });

    it('forwards components and strict to the controller broker', async () => {
      const cluster = fakeCluster({
        describeClientQuotas: vi.fn().mockResolvedValue({ entries: [] }),
      });
      const api = makeApi(cluster);
      const components: DescribeClientQuotasComponent[] = [{ entityType: 'client-id', matchType: 0, match: 'app-1' }];

      await api.describeClientQuotas({ components, strict: true });

      expect(cluster.broker.describeClientQuotas).toHaveBeenCalledWith({ components, strict: true });
    });

    it('propagates a non-retriable broker error', async () => {
      const cluster = fakeCluster({
        describeClientQuotas: vi.fn().mockRejectedValue(new Error('boom')),
      });
      const api = makeApi(cluster);

      await expect(api.describeClientQuotas()).rejects.toThrow('boom');
    });

    it('refreshes metadata and retries when the controller returns NOT_CONTROLLER', async () => {
      const notController = Object.assign(new Error('not controller'), { type: 'NOT_CONTROLLER' });
      const cluster = fakeCluster({
        describeClientQuotas: vi.fn().mockRejectedValue(notController),
      });
      const api = makeApi(cluster, { retries: 0 });

      await expect(api.describeClientQuotas()).rejects.toThrow();
      expect(cluster.broker.describeClientQuotas).toHaveBeenCalledTimes(1);
      expect(cluster.refreshMetadata).toHaveBeenCalledTimes(2);
    });
  });

  describe('alterClientQuotas', () => {
    it('rejects a non-array entries', async () => {
      const api = makeApi(fakeCluster());
      await expect(
        api.alterClientQuotas({ entries: undefined as unknown as AlterClientQuotasEntry[] }),
      ).rejects.toThrow('Entries array cannot be empty');
    });

    it('rejects an empty entries array', async () => {
      const cluster = fakeCluster();
      const api = makeApi(cluster);
      await expect(api.alterClientQuotas({ entries: [] })).rejects.toThrow(KafkaNonRetriableError);
      expect(cluster.findControllerBroker).not.toHaveBeenCalled();
    });

    it('alters client quotas through the active controller with validateOnly defaulted to false', async () => {
      const cluster = fakeCluster({
        alterClientQuotas: vi.fn().mockResolvedValue({ entries: [{ entity: [], errorCode: 0 }] }),
      });
      const api = makeApi(cluster);

      await expect(api.alterClientQuotas({ entries: [validEntry] })).resolves.toEqual({
        entries: [{ entity: [], errorCode: 0 }],
      });

      expect(cluster.broker.alterClientQuotas).toHaveBeenCalledWith({
        entries: [validEntry],
        validateOnly: false,
      });
    });

    it('forwards an explicit validateOnly flag', async () => {
      const cluster = fakeCluster({
        alterClientQuotas: vi.fn().mockResolvedValue({ entries: [] }),
      });
      const api = makeApi(cluster);

      await api.alterClientQuotas({ entries: [validEntry], validateOnly: true });

      expect(cluster.broker.alterClientQuotas).toHaveBeenCalledWith({
        entries: [validEntry],
        validateOnly: true,
      });
    });

    it('propagates a non-retriable broker error', async () => {
      const cluster = fakeCluster({
        alterClientQuotas: vi.fn().mockRejectedValue(new Error('boom')),
      });
      const api = makeApi(cluster);

      await expect(api.alterClientQuotas({ entries: [validEntry] })).rejects.toThrow('boom');
    });

    it('refreshes metadata and retries when the controller returns NOT_CONTROLLER', async () => {
      const notController = Object.assign(new Error('not controller'), { type: 'NOT_CONTROLLER' });
      const cluster = fakeCluster({
        alterClientQuotas: vi.fn().mockRejectedValue(notController),
      });
      const api = makeApi(cluster, { retries: 0 });

      await expect(api.alterClientQuotas({ entries: [validEntry] })).rejects.toThrow();
      expect(cluster.broker.alterClientQuotas).toHaveBeenCalledTimes(1);
      expect(cluster.refreshMetadata).toHaveBeenCalledTimes(2);
    });
  });
});
