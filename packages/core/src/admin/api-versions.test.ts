import { describe, expect, it, vi } from 'vitest';
import type { Cluster } from '../cluster/index';
import { nodeTarget } from '../cluster/run';
import { fakeRunCluster } from '../cluster/test-run';
import { KafkaNonRetriableError } from '../errors';
import { createLogger, LOG_LEVELS } from '../loggers/index';
import { API_KEYS } from '../protocol/requests/api-keys';
import type { BrokerVersions } from '../protocol/requests/index';
import { createApiVersionsApi } from './api-versions';

const silentLogger = createLogger({ level: LOG_LEVELS.NOTHING, logCreator: () => () => {} });

function makeApi(cluster: object) {
  return createApiVersionsApi({
    cluster: cluster as unknown as Cluster,
    logger: silentLogger.namespace('Admin'),
    rootLogger: silentLogger,
  });
}

function fakeBroker(versions: BrokerVersions, { host = 'localhost', port = 9092, rack = null as string | null } = {}) {
  return { apiVersions: vi.fn().mockResolvedValue(versions), connectionPool: { host, port, rack } };
}

describe('admin/api-versions', () => {
  it('reports each API with the broker range and the version this client negotiates', async () => {
    const broker = fakeBroker({
      [API_KEYS.Produce]: { minVersion: 3, maxVersion: 99 },
      [API_KEYS.ApiVersions]: { minVersion: 0, maxVersion: 2 },
      // Inter-broker API this client never sends.
      [API_KEYS.BrokerHeartbeat]: { minVersion: 0, maxVersion: 1 },
      // An api key newer than this client knows.
      [999]: { minVersion: 0, maxVersion: 0 },
    });
    const cluster = fakeRunCluster({ node: broker }, { brokerPool: { brokers: { '1': {} } } });

    const { brokers } = await makeApi(cluster).describeApiVersions();

    expect(brokers).toHaveLength(1);
    const [described] = brokers;
    expect(described).toMatchObject({ nodeId: 1, host: 'localhost', port: 9092, rack: null });
    const byKey = new Map(described?.apis.map((api) => [api.apiKey, api]));
    expect(byKey.get(API_KEYS.Produce)).toStrictEqual({
      apiKey: 0,
      apiName: 'Produce',
      brokerVersions: { minVersion: 3, maxVersion: 99 },
      isImplemented: true,
      version: 13,
    });
    expect(byKey.get(API_KEYS.ApiVersions)).toMatchObject({ version: 2, isImplemented: true });
    expect(byKey.get(API_KEYS.BrokerHeartbeat)).toStrictEqual({
      apiKey: 63,
      apiName: 'BrokerHeartbeat',
      brokerVersions: { minVersion: 0, maxVersion: 1 },
      isImplemented: false,
    });
    expect(byKey.get(999)).toStrictEqual({
      apiKey: 999,
      brokerVersions: { minVersion: 0, maxVersion: 0 },
      isImplemented: false,
    });
    // Implemented by this client but not advertised by the broker.
    expect(byKey.get(API_KEYS.Fetch)).toStrictEqual({ apiKey: 1, apiName: 'Fetch', isImplemented: true });
    // Named as the protocol names it, not by this client's GroupCoordinator.
    expect(byKey.get(API_KEYS.GroupCoordinator)?.apiName).toBe('FindCoordinator');
    // A lazy admin family counts as implemented.
    expect(byKey.get(API_KEYS.UnregisterController)).toMatchObject({ isImplemented: true });
    expect(described?.apis.map((api) => api.apiKey)).toEqual(
      [...(described?.apis.map((api) => api.apiKey) ?? [])].sort((a, b) => a - b),
    );
  });

  it('leaves out the version when the broker shares none with this client', async () => {
    const broker = fakeBroker({ [API_KEYS.Produce]: { minVersion: 50, maxVersion: 60 } });
    const cluster = fakeRunCluster({ node: broker }, { brokerPool: { brokers: { '1': {} } } });

    const { brokers } = await makeApi(cluster).describeApiVersions();

    expect(brokers[0]?.apis.find((api) => api.apiKey === API_KEYS.Produce)).toStrictEqual({
      apiKey: 0,
      apiName: 'Produce',
      brokerVersions: { minVersion: 50, maxVersion: 60 },
      isImplemented: true,
    });
  });

  it('asks every broker in the metadata by default, with a fresh ApiVersions request each', async () => {
    const brokers = {
      '1': fakeBroker({}, { host: 'kafka-1', port: 9092, rack: 'a' }),
      '2': fakeBroker({}, { host: 'kafka-2', port: 9093, rack: 'b' }),
    };
    const cluster = fakeRunCluster(
      { node: (nodeId) => brokers[nodeId as '1' | '2'] },
      { brokerPool: { brokers: { '1': {}, '2': {} } } },
    );

    const result = await makeApi(cluster).describeApiVersions({ brokerIds: [] });

    expect(result.brokers.map(({ nodeId, host, port, rack }) => ({ nodeId, host, port, rack }))).toEqual([
      { nodeId: 1, host: 'kafka-1', port: 9092, rack: 'a' },
      { nodeId: 2, host: 'kafka-2', port: 9093, rack: 'b' },
    ]);
    expect(brokers['1'].apiVersions).toHaveBeenCalledOnce();
    expect(brokers['2'].apiVersions).toHaveBeenCalledOnce();
    expect(cluster.refreshMetadata).toHaveBeenCalled();
  });

  it('asks only the requested brokers', async () => {
    const cluster = fakeRunCluster({ node: fakeBroker({}) }, { brokerPool: { brokers: { '1': {}, '2': {} } } });

    const { brokers } = await makeApi(cluster).describeApiVersions({ brokerIds: [2] });

    expect(brokers.map(({ nodeId }) => nodeId)).toEqual([2]);
    expect(cluster.resolveTarget).toHaveBeenCalledExactlyOnceWith(nodeTarget(2));
  });

  it('rejects a broker id missing from the metadata without retrying', async () => {
    const cluster = fakeRunCluster({ node: fakeBroker({}) }, { brokerPool: { brokers: { '1': {} } } });

    await expect(makeApi(cluster).describeApiVersions({ brokerIds: [7] })).rejects.toThrow(
      'Broker 7 is not in the cluster metadata',
    );
    expect(cluster.refreshMetadata).toHaveBeenCalledOnce();
    expect(cluster.resolveTarget).not.toHaveBeenCalled();
  });

  it('throws when no brokers are available', async () => {
    const cluster = fakeRunCluster({}, { brokerPool: { brokers: {} } });

    await expect(makeApi(cluster).describeApiVersions()).rejects.toThrow(
      'No brokers available to describe API versions',
    );
  });

  it.each([[[-1]], [[1.5]], [['1']], ['1']])(
    'rejects brokerIds %p before contacting the cluster',
    async (brokerIds) => {
      const cluster = fakeRunCluster({}, { brokerPool: { brokers: {} } });

      await expect(makeApi(cluster).describeApiVersions({ brokerIds: brokerIds as number[] })).rejects.toThrow(
        KafkaNonRetriableError,
      );
      expect(cluster.run).not.toHaveBeenCalled();
    },
  );
});
