import { afterEach, describe, expect, it } from 'vitest';
import { Broker } from '../../../src/broker/index';
import type { Cluster } from '../../../src/cluster/index';
import { nodeTarget } from '../../../src/cluster/run';
import { createConnectionPool, createCluster, newLogger, testIfKafkaAtLeast_4_4 } from '../../helpers/index';

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

  testIfKafkaAtLeast_4_4(
    'rediscovers the cluster when a broker address leads to another broker (KIP-1242)',
    async () => {
      cluster = createCluster();
      await cluster.connect();
      await cluster.refreshMetadata();

      const [nodeId, otherNodeId] = cluster.brokerPool.getNodeIds();
      const { host, port } = cluster.brokerPool.brokers[nodeId!]!.connectionPool;
      const seedBefore = cluster.brokerPool.seedBroker;

      // Simulate the address of broker `nodeId` being reused by broker `otherNodeId`.
      const reusedAddress = cluster.brokerPool.brokers[otherNodeId!]!.connectionPool;
      await cluster.brokerPool.brokers[nodeId!]!.disconnect();
      cluster.brokerPool.brokers[nodeId!] = new Broker({
        connectionPool: createConnectionPool({ host: reusedAddress.host, port: reusedAddress.port }),
        logger: newLogger(),
        nodeId: Number(nodeId),
        clusterId: cluster.brokerPool.metadata!.clusterId,
        versions: cluster.brokerPool.versions,
      });

      // The first attempt meets the reused address, rediscovers the cluster and fails as retriable;
      // the retry resolves the broker again from the fresh metadata.
      const reached = await cluster.run(nodeTarget(nodeId!), async (broker) => broker, { retryRetriable: true });

      expect(cluster.brokerPool.seedBroker).not.toBe(seedBefore);
      expect(reached.nodeId).toBe(Number(nodeId));
      expect({ host: reached.connectionPool.host, port: reached.connectionPool.port }).toEqual({ host, port });
    },
  );
});
