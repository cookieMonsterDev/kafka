import { describe, expect, it, vi } from 'vitest';
import { Broker } from '../broker/index.js';
import { createLogger, LOG_LEVELS } from '../loggers/index.js';
import { KafkaJSConnectionError } from '../errors.js';
import type { ConnectionPool } from '../network/connection-pool.js';
import { BrokerPool } from './broker-pool.js';
import type { ConnectionPoolBuilder, ConnectionPoolDestination } from './connection-pool-builder.js';

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

function fakeBuilder(build: (destination?: ConnectionPoolDestination) => Promise<ConnectionPool>): ConnectionPoolBuilder {
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

  it('connect rotates the seed broker on KafkaJSConnectionError and eventually succeeds', async () => {
    let attempt = 0;
    const build = vi.fn(async () => {
      attempt += 1;
      const isFirstAttempt = attempt === 1;
      return fakeConnectionPool({
        getConnection: vi.fn().mockImplementation(() => {
          if (isFirstAttempt) throw new KafkaJSConnectionError('refused');
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
    brokerPool.metadata = { brokers: [], topicMetadata: [], throttleTime: 0, clusterId: null, controllerId: 0, clientSideThrottleTime: 0 };

    await brokerPool.disconnect();

    expect(destroySpy).toHaveBeenCalledOnce();
    expect(brokerPool.brokers).toEqual({});
    expect(brokerPool.metadata).toBeNull();
  });

  it('removeBroker drops the matching broker and reassigns the seed if it was removed', async () => {
    const seedPool = fakeConnectionPool({ host: 'seed-host', port: 9092 });
    const brokerPool = new BrokerPool({ connectionPoolBuilder: fakeBuilder(async () => seedPool), logger: silentLogger });
    await brokerPool.createSeedBroker();
    brokerPool.seedBroker!.nodeId = 1;
    brokerPool.brokers = { '1': brokerPool.seedBroker! };

    brokerPool.removeBroker({ host: 'seed-host', port: 9092 });

    expect(brokerPool.brokers['1']).toBeUndefined();
  });

  describe('findBroker / withBroker / findConnectedBroker', () => {
    it('findBroker throws KafkaJSBrokerNotFound for an unknown nodeId', async () => {
      const brokerPool = new BrokerPool({
        connectionPoolBuilder: fakeBuilder(async () => fakeConnectionPool()),
        logger: silentLogger,
      });
      await expect(brokerPool.findBroker({ nodeId: 'missing' })).rejects.toThrow('not found in the cached metadata');
    });

    it('withBroker throws KafkaJSBrokerNotFound when the pool is empty', async () => {
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
});
