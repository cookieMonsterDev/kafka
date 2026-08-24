import { describe, expect, it, vi } from 'vitest';
import { Broker } from '../broker/index';
import { createLogger, LOG_LEVELS } from '../loggers/index';
import { KafkaConnectionError } from '../errors';
import type { ConnectionPool } from '../network/connection-pool';
import { BrokerPool } from './broker-pool';
import type { ConnectionPoolBuilder, ConnectionPoolDestination } from './connection-pool-builder';

const silentLogger = createLogger({ level: LOG_LEVELS.NOTHING, logCreator: () => () => {} });

function fakeConnectionPool(overrides: Partial<Record<string, unknown>> = {}): ConnectionPool {
  return {
    host: 'broker-1',
    port: 9092,
    rack: null,
    connectionTimeout: 1000,
    sasl: null,
    isConnected: vi.fn().mockReturnValue(false),
    isAuthenticated: vi.fn().mockReturnValue(false),
    getConnection: vi.fn().mockResolvedValue({
      getSupportAuthenticationProtocol: vi.fn().mockReturnValue(true),
      authenticate: vi.fn().mockResolvedValue(undefined),
    }),
    setVersions: vi.fn(),
    setSupportAuthenticationProtocol: vi.fn(),
    send: vi.fn().mockResolvedValue({ errorCode: 0, throttleTime: 0, apiVersions: [] }),
    destroy: vi.fn().mockResolvedValue(undefined),
    ...overrides,
  } as unknown as ConnectionPool;
}

function fakeBuilder(
  build: (destination?: ConnectionPoolDestination) => Promise<ConnectionPool>,
): ConnectionPoolBuilder {
  return { build };
}

describe('cluster/BrokerPool', () => {
  it('hasConnectedBrokers reflects the seed broker before any real broker exists', async () => {
    const pool = fakeConnectionPool({ isConnected: vi.fn().mockReturnValue(true) });
    const brokerPool = new BrokerPool({
      connectionPoolBuilder: fakeBuilder(async () => pool),
      logger: silentLogger,
    });

    expect(brokerPool.hasConnectedBrokers()).toBe(false);
    await brokerPool.createSeedBroker();
    expect(brokerPool.hasConnectedBrokers()).toBe(true);
  });

  it('connect rotates the seed broker on KafkaConnectionError and eventually succeeds', async () => {
    let attempt = 0;
    const build = vi.fn(async () => {
      attempt += 1;
      const isFirstAttempt = attempt === 1;
      return fakeConnectionPool({
        getConnection: vi.fn().mockImplementation(() => {
          if (isFirstAttempt) throw new KafkaConnectionError('refused');
          return Promise.resolve({
            getSupportAuthenticationProtocol: vi.fn().mockReturnValue(true),
            authenticate: vi.fn().mockResolvedValue(undefined),
          });
        }),
      });
    });

    const brokerPool = new BrokerPool({
      connectionPoolBuilder: fakeBuilder(build),
      logger: silentLogger,
      retry: { retries: 2, initialRetryTime: 1, maxRetryTime: 5 },
    });

    await brokerPool.connect();
    expect(build).toHaveBeenCalledTimes(2);
  });

  it('disconnect tears down the seed broker and every known broker, clearing state', async () => {
    const destroySpy = vi.fn().mockResolvedValue(undefined);
    const pool = fakeConnectionPool({ destroy: destroySpy });
    const brokerPool = new BrokerPool({ connectionPoolBuilder: fakeBuilder(async () => pool), logger: silentLogger });
    await brokerPool.createSeedBroker();
    brokerPool.metadata = {
      brokers: [],
      topicMetadata: [],
      throttleTime: 0,
      clusterId: null,
      controllerId: 0,
      clientSideThrottleTime: 0,
      clusterAuthorizedOperations: -2147483648,
    };

    await brokerPool.disconnect();

    expect(destroySpy).toHaveBeenCalledOnce();
    expect(brokerPool.brokers).toEqual({});
    expect(brokerPool.metadata).toBeNull();
  });

  it('removeBroker drops the matching broker and reassigns the seed if it was removed', async () => {
    const seedPool = fakeConnectionPool({ host: 'seed-host', port: 9092 });
    const brokerPool = new BrokerPool({
      connectionPoolBuilder: fakeBuilder(async () => seedPool),
      logger: silentLogger,
    });
    await brokerPool.createSeedBroker();
    brokerPool.seedBroker!.nodeId = 1;
    brokerPool.brokers = { '1': brokerPool.seedBroker! };

    brokerPool.removeBroker({ host: 'seed-host', port: 9092 });

    expect(brokerPool.brokers['1']).toBeUndefined();
  });

  describe('findBroker / withBroker / findConnectedBroker', () => {
    it('findBroker throws KafkaBrokerNotFound for an unknown nodeId', async () => {
      const brokerPool = new BrokerPool({
        connectionPoolBuilder: fakeBuilder(async () => fakeConnectionPool()),
        logger: silentLogger,
      });
      await expect(brokerPool.findBroker({ nodeId: 'missing' })).rejects.toThrow('not found in the cached metadata');
    });

    it('withBroker throws KafkaBrokerNotFound when the pool is empty', async () => {
      const brokerPool = new BrokerPool({
        connectionPoolBuilder: fakeBuilder(async () => fakeConnectionPool()),
        logger: silentLogger,
      });
      await expect(brokerPool.withBroker(async () => 'x')).rejects.toThrow('No brokers in the broker pool');
    });

    it('withBroker returns the callback result from the first broker that succeeds', async () => {
      const connectedPool = fakeConnectionPool({ isConnected: vi.fn().mockReturnValue(true) });
      const brokerPool = new BrokerPool({
        connectionPoolBuilder: fakeBuilder(async () => connectedPool),
        logger: silentLogger,
      });
      await brokerPool.createSeedBroker();
      brokerPool.brokers = { '1': brokerPool.seedBroker! };

      const result = await brokerPool.withBroker(async ({ nodeId }) => `handled-${nodeId}`);
      expect(result).toBe('handled-1');
    });
  });

  describe('refreshMetadata', () => {
    it('assigns the seed broker its nodeId/rack and creates a new broker for every other address', async () => {
      const seedPool = fakeConnectionPool({ host: 'seed-host', port: 9092 });
      const metadataResponse = {
        errorCode: 0,
        throttleTime: 0,
        apiVersions: Array.from({ length: 50 }, (_, apiKey) => ({ apiKey, minVersion: 0, maxVersion: 99 })),
      };
      seedPool.send = vi
        .fn()
        .mockResolvedValueOnce(metadataResponse) // apiVersions negotiated during connect()
        .mockResolvedValueOnce({
          brokers: [
            { nodeId: 1, host: 'seed-host', port: 9092, rack: 'rack-a' },
            { nodeId: 2, host: 'other-host', port: 9093, rack: null },
          ],
          topicMetadata: [],
          throttleTime: 0,
          clusterId: null,
          controllerId: 1,
          clientSideThrottleTime: 0,
          clusterAuthorizedOperations: -2147483648,
        }); // Metadata response inside refreshMetadata

      const otherPool = fakeConnectionPool({ host: 'other-host', port: 9093 });
      const build = vi.fn(async (destination?: ConnectionPoolDestination) =>
        destination?.host === 'other-host' ? otherPool : seedPool,
      );

      const brokerPool = new BrokerPool({ connectionPoolBuilder: fakeBuilder(build), logger: silentLogger });
      await brokerPool.connect();
      await brokerPool.refreshMetadata([]);

      expect(brokerPool.brokers['1']).toBe(brokerPool.seedBroker);
      expect(brokerPool.seedBroker!.nodeId).toBe(1);
      expect(brokerPool.seedBroker!.connectionPool.rack).toBe('rack-a');
      expect(brokerPool.brokers['2']).toBeDefined();
      expect(brokerPool.brokers['2']).not.toBe(brokerPool.seedBroker);
    });

    it('disconnects brokers no longer present in fresh metadata', async () => {
      const seedPool = fakeConnectionPool({ host: 'seed-host', port: 9092 });
      const staleDestroySpy = vi.fn().mockResolvedValue(undefined);
      const staleBrokerPool = fakeConnectionPool({ host: 'stale-host', port: 9094, destroy: staleDestroySpy });
      const freshMetadataOnly = (brokers: { nodeId: number; host: string; port: number; rack: string | null }[]) => ({
        brokers,
        topicMetadata: [],
        throttleTime: 0,
        clusterId: null,
        controllerId: 1,
        clientSideThrottleTime: 0,
        clusterAuthorizedOperations: -2147483648,
      });

      seedPool.send = vi
        .fn()
        .mockResolvedValueOnce({
          errorCode: 0,
          throttleTime: 0,
          apiVersions: Array.from({ length: 50 }, (_, apiKey) => ({ apiKey, minVersion: 0, maxVersion: 99 })),
        }) // apiVersions negotiated during connect()
        .mockResolvedValueOnce(freshMetadataOnly([{ nodeId: 1, host: 'seed-host', port: 9092, rack: null }])) // first refreshMetadata: establishes broker '1' = seed
        .mockResolvedValueOnce(freshMetadataOnly([{ nodeId: 1, host: 'seed-host', port: 9092, rack: null }])); // second refreshMetadata: broker '99' is gone

      const brokerPool = new BrokerPool({
        connectionPoolBuilder: fakeBuilder(async () => seedPool),
        logger: silentLogger,
      });
      await brokerPool.connect();
      await brokerPool.refreshMetadata([]);
      expect(brokerPool.brokers['1']).toBe(brokerPool.seedBroker);

      // The seed is now the connected broker findConnectedBroker() should deterministically pick,
      // instead of racing shuffle order against the (not yet connected) stale broker below.
      seedPool.isConnected = vi.fn().mockReturnValue(true);

      // Inject an extra broker as if a previous refresh had discovered it.
      brokerPool.brokers['99'] = new Broker({ connectionPool: staleBrokerPool, logger: silentLogger });

      await brokerPool.refreshMetadata([]);

      expect(staleDestroySpy).toHaveBeenCalledOnce();
      expect(brokerPool.brokers['99']).toBeUndefined();
    });
  });

  describe('rebootstrap (KIP-1102)', () => {
    function apiVersionsResponse() {
      return {
        errorCode: 0,
        throttleTime: 0,
        apiVersions: Array.from({ length: 50 }, (_, apiKey) => ({ apiKey, minVersion: 0, maxVersion: 99 })),
      };
    }

    function rebootstrapRequiredError() {
      return Object.assign(new Error('Client metadata is stale'), {
        name: 'KafkaProtocolError',
        type: 'REBOOTSTRAP_REQUIRED',
        retriable: false,
      });
    }

    it('rebootstrap disconnects known brokers, clears metadata, and rebuilds the seed from the bootstrap list', async () => {
      const oldSeedPool = fakeConnectionPool({ host: 'old-seed', port: 9092 });
      const newSeedPool = fakeConnectionPool({ host: 'new-seed', port: 9092 });
      const staleBrokerDestroy = vi.fn().mockResolvedValue(undefined);
      const staleBrokerPool = fakeConnectionPool({ host: 'stale-broker', port: 9094, destroy: staleBrokerDestroy });

      const pools = [oldSeedPool, newSeedPool];
      let buildCount = 0;
      const build = vi.fn(async () => pools[buildCount++]!);

      const brokerPool = new BrokerPool({ connectionPoolBuilder: fakeBuilder(build), logger: silentLogger });
      await brokerPool.createSeedBroker();
      brokerPool.brokers = { '99': new Broker({ connectionPool: staleBrokerPool, logger: silentLogger }) };
      brokerPool.metadata = {
        brokers: [],
        topicMetadata: [],
        throttleTime: 0,
        clusterId: null,
        controllerId: 0,
        clientSideThrottleTime: 0,
        clusterAuthorizedOperations: -2147483648,
      };

      await brokerPool.rebootstrap();

      expect(staleBrokerDestroy).toHaveBeenCalledOnce();
      expect(brokerPool.brokers).toEqual({});
      expect(brokerPool.metadata).toBeNull();
      expect(brokerPool.seedBroker!.connectionPool.host).toBe('new-seed');
      expect(build).toHaveBeenCalledTimes(2);
    });

    it('refreshMetadata rebootstraps from the seed list on REBOOTSTRAP_REQUIRED and recovers', async () => {
      const staleSeedPool = fakeConnectionPool({ host: 'stale-seed', port: 9092 });
      const freshSeedPool = fakeConnectionPool({ host: 'fresh-seed', port: 9092 });

      staleSeedPool.send = vi
        .fn()
        .mockResolvedValueOnce(apiVersionsResponse()) // apiVersions negotiated on the initial connect()
        .mockRejectedValueOnce(rebootstrapRequiredError()); // the Metadata RPC itself signals rebootstrap

      const metadataResponse = {
        brokers: [{ nodeId: 1, host: 'fresh-seed', port: 9092, rack: null }],
        topicMetadata: [],
        throttleTime: 0,
        clusterId: null,
        controllerId: 1,
        clientSideThrottleTime: 0,
        clusterAuthorizedOperations: -2147483648,
      };
      freshSeedPool.send = vi
        .fn()
        .mockResolvedValueOnce(apiVersionsResponse()) // apiVersions negotiated while rebootstrapping
        .mockResolvedValueOnce(metadataResponse); // Metadata succeeds against the fresh seed

      const pools = [staleSeedPool, freshSeedPool];
      let buildCount = 0;
      const build = vi.fn(async () => pools[buildCount++]!);

      const brokerPool = new BrokerPool({
        connectionPoolBuilder: fakeBuilder(build),
        logger: silentLogger,
        retry: { retries: 2, initialRetryTime: 1, maxRetryTime: 5 },
      });

      await brokerPool.connect();
      await brokerPool.refreshMetadata([]);

      expect(brokerPool.seedBroker!.connectionPool.host).toBe('fresh-seed');
      expect(brokerPool.metadata).toEqual(metadataResponse);
      expect(build).toHaveBeenCalledTimes(2);
    });

    it('metadataRecovery: "none" does not rebootstrap and surfaces REBOOTSTRAP_REQUIRED as non-retriable', async () => {
      const seedPool = fakeConnectionPool({ host: 'seed-host', port: 9092 });
      seedPool.send = vi
        .fn()
        .mockResolvedValueOnce(apiVersionsResponse())
        .mockRejectedValueOnce(rebootstrapRequiredError());

      const build = vi.fn(async () => seedPool);
      const brokerPool = new BrokerPool({
        connectionPoolBuilder: fakeBuilder(build),
        logger: silentLogger,
        metadataRecovery: 'none',
        retry: { retries: 2, initialRetryTime: 1, maxRetryTime: 5 },
      });

      await brokerPool.connect();
      await expect(brokerPool.refreshMetadata([])).rejects.toThrow();
      expect(build).toHaveBeenCalledTimes(1);
    });
  });

  describe('applyLeaderUpdate', () => {
    function metadataFixture(partitionMetadata: { partitionId: number; leader: number }[]) {
      return {
        brokers: [{ nodeId: 1, host: 'broker-1', port: 9092, rack: null }],
        topicMetadata: [
          {
            topic: 'orders',
            topicErrorCode: 0,
            isInternal: false,
            partitionMetadata: partitionMetadata.map((p) => ({
              partitionErrorCode: 0,
              partitionId: p.partitionId,
              leader: p.leader,
              replicas: [],
              isr: [],
            })),
          },
        ],
        throttleTime: 0,
        clusterId: null,
        controllerId: 1,
        clientSideThrottleTime: 0,
        clusterAuthorizedOperations: -2147483648,
      };
    }

    it('patches the cached partition leader without a Metadata RPC', async () => {
      const brokerPool = new BrokerPool({
        connectionPoolBuilder: fakeBuilder(async () => fakeConnectionPool()),
        logger: silentLogger,
      });
      brokerPool.metadata = metadataFixture([{ partitionId: 0, leader: 1 }]);

      const applied = await brokerPool.applyLeaderUpdate({
        topic: 'orders',
        partition: 0,
        currentLeader: { leaderId: 2, leaderEpoch: 5 },
      });

      expect(applied).toBe(true);
      expect(brokerPool.metadata.topicMetadata[0]!.partitionMetadata[0]).toMatchObject({
        leader: 2,
        leaderEpoch: 5,
      });
    });

    it('registers a new broker from NodeEndpoints without a Metadata RPC', async () => {
      const brokerPool = new BrokerPool({
        connectionPoolBuilder: fakeBuilder(async (destination) => fakeConnectionPool({ host: destination?.host })),
        logger: silentLogger,
      });
      brokerPool.metadata = metadataFixture([{ partitionId: 0, leader: 1 }]);

      const applied = await brokerPool.applyLeaderUpdate({
        topic: 'orders',
        partition: 0,
        currentLeader: { leaderId: 2, leaderEpoch: 5 },
        nodeEndpoints: [{ nodeId: 2, host: 'broker-2', port: 9093, rack: null }],
      });

      expect(applied).toBe(true);
      expect(brokerPool.brokers['2']).toBeDefined();
      expect(brokerPool.brokers['2']!.connectionPool.host).toBe('broker-2');
      expect(brokerPool.metadata.brokers).toContainEqual({
        nodeId: 2,
        host: 'broker-2',
        port: 9093,
        rack: null,
      });
    });

    it('returns false without patching when there is no cached metadata yet', async () => {
      const brokerPool = new BrokerPool({
        connectionPoolBuilder: fakeBuilder(async () => fakeConnectionPool()),
        logger: silentLogger,
      });

      const applied = await brokerPool.applyLeaderUpdate({
        topic: 'orders',
        partition: 0,
        currentLeader: { leaderId: 2, leaderEpoch: 5 },
      });

      expect(applied).toBe(false);
    });

    it('returns false when the partition is not in the cached metadata', async () => {
      const brokerPool = new BrokerPool({
        connectionPoolBuilder: fakeBuilder(async () => fakeConnectionPool()),
        logger: silentLogger,
      });
      brokerPool.metadata = metadataFixture([{ partitionId: 0, leader: 1 }]);

      const applied = await brokerPool.applyLeaderUpdate({
        topic: 'orders',
        partition: 99,
        currentLeader: { leaderId: 2, leaderEpoch: 5 },
      });

      expect(applied).toBe(false);
    });

    it('ignores a negative leaderId (no known leader) and does not patch', async () => {
      const brokerPool = new BrokerPool({
        connectionPoolBuilder: fakeBuilder(async () => fakeConnectionPool()),
        logger: silentLogger,
      });
      brokerPool.metadata = metadataFixture([{ partitionId: 0, leader: 1 }]);

      const applied = await brokerPool.applyLeaderUpdate({
        topic: 'orders',
        partition: 0,
        currentLeader: { leaderId: -1, leaderEpoch: -1 },
      });

      expect(applied).toBe(false);
      expect(brokerPool.metadata.topicMetadata[0]!.partitionMetadata[0]!.leader).toBe(1);
    });
  });
});
