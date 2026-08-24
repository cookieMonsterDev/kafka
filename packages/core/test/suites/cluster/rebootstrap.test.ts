import { afterEach, describe, expect, it } from 'vitest';
import { Broker } from '../../../src/broker/index';
import type { Cluster } from '../../../src/cluster/index';
import { createConnectionPool, createCluster, newLogger } from '../../helpers/index';

describe('cluster.rebootstrap (KIP-1102)', () => {
  let cluster: Cluster | undefined;

  afterEach(async () => {
    await cluster?.disconnect();
  });

  it('rediscovers the real cluster from the original bootstrap list once its cached brokers are wiped', async () => {
    cluster = createCluster();
    await cluster.connect();
    await cluster.refreshMetadata();

    const discoveredNodeIds = cluster.brokerPool.getNodeIds();
    expect(discoveredNodeIds.length).toBeGreaterThan(0);

    // Simulate a wiped/stale advertised broker set (e.g. after a long partition or DNS change):
    // every cached broker, including the seed, now points somewhere nothing is listening.
    const bogusConnectionPool = () => createConnectionPool({ host: '127.0.0.1', port: 1, connectionTimeout: 200 });

    for (const nodeId of discoveredNodeIds) {
      await cluster.brokerPool.brokers[nodeId]!.disconnect();
      cluster.brokerPool.brokers[nodeId] = new Broker({ connectionPool: bogusConnectionPool(), logger: newLogger() });
    }
    await cluster.brokerPool.seedBroker!.disconnect();
    cluster.brokerPool.seedBroker = new Broker({ connectionPool: bogusConnectionPool(), logger: newLogger() });

    expect(cluster.brokerPool.hasConnectedBrokers()).toBe(false);

    // `rebootstrap()` must rediscover the cluster from the connectionPoolBuilder's original seed
    // list, not from any of the (now bogus) addresses cached above.
    await cluster.brokerPool.rebootstrap();

    expect(cluster.brokerPool.hasConnectedBrokers()).toBe(true);
    expect(cluster.brokerPool.metadata).toBeNull();

    await cluster.refreshMetadata();

    expect(cluster.brokerPool.metadata).not.toBeNull();
    expect(cluster.brokerPool.getNodeIds().length).toBeGreaterThan(0);
  });
});
