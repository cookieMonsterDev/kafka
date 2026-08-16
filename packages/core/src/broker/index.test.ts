import { describe, expect, it, vi } from 'vitest';
import { KafkaConnectionClosedError, KafkaMemberIdRequired, KafkaProtocolError } from '../errors';
import { createLogger, LOG_LEVELS } from '../loggers/index';
import type { ConnectionPool } from '../network/connection-pool';
import { API_KEYS } from '../protocol/requests/api-keys';
import { Broker } from './index';

const silentLogger = createLogger({ level: LOG_LEVELS.NOTHING, logCreator: () => () => {} });

interface FakeConnectionPool {
  host: string;
  port: number;
  connectionTimeout: number;
  sasl: unknown;
  isConnected: ReturnType<typeof vi.fn>;
  isAuthenticated: ReturnType<typeof vi.fn>;
  getConnection: ReturnType<typeof vi.fn>;
  setVersions: ReturnType<typeof vi.fn>;
  setSupportAuthenticationProtocol: ReturnType<typeof vi.fn>;
  send: ReturnType<typeof vi.fn>;
  destroy: ReturnType<typeof vi.fn>;
}

function createFakeConnectionPool(overrides: Partial<FakeConnectionPool> = {}): FakeConnectionPool {
  return {
    host: '127.0.0.1',
    port: 9092,
    connectionTimeout: 1000,
    sasl: null,
    isConnected: vi.fn().mockReturnValue(false),
    isAuthenticated: vi.fn().mockReturnValue(false),
    getConnection: vi.fn().mockResolvedValue({
      getSupportAuthenticationProtocol: vi.fn().mockReturnValue(null),
      authenticate: vi.fn().mockResolvedValue(undefined),
    }),
    setVersions: vi.fn(),
    setSupportAuthenticationProtocol: vi.fn(),
    send: vi.fn(),
    destroy: vi.fn().mockResolvedValue(undefined),
    ...overrides,
  };
}

const asConnectionPool = (fake: FakeConnectionPool): ConnectionPool => fake as unknown as ConnectionPool;

describe('broker/Broker', () => {
  describe('constructor', () => {
    it('derives the broker address from the connection pool', () => {
      const pool = createFakeConnectionPool({ host: 'kafka-1', port: 9093 });
      const broker = new Broker({ connectionPool: asConnectionPool(pool), logger: silentLogger });
      expect(broker.brokerAddress).toBe('kafka-1:9093');
    });

    it('throws "Broker not connected" if a request is attempted before connect()', () => {
      const pool = createFakeConnectionPool();
      const broker = new Broker({ connectionPool: asConnectionPool(pool), logger: silentLogger });
      expect(() => broker.lookupRequest(API_KEYS.Metadata, {} as never)).toThrow('Broker not connected');
    });
  });

  describe('isConnected', () => {
    it('only requires isConnected() when SASL is not configured', () => {
      const pool = createFakeConnectionPool({ sasl: null, isConnected: vi.fn().mockReturnValue(true) });
      const broker = new Broker({ connectionPool: asConnectionPool(pool), logger: silentLogger });
      expect(broker.isConnected()).toBe(true);
    });

    it('requires both isConnected() and isAuthenticated() when SASL is configured', () => {
      const pool = createFakeConnectionPool({
        sasl: { mechanism: 'plain' },
        isConnected: vi.fn().mockReturnValue(true),
        isAuthenticated: vi.fn().mockReturnValue(false),
      });
      const broker = new Broker({ connectionPool: asConnectionPool(pool), logger: silentLogger });
      expect(broker.isConnected()).toBe(false);
    });
  });

  describe('apiVersions', () => {
    it('tries the highest implemented version first and stops on success', async () => {
      const pool = createFakeConnectionPool();
      pool.send.mockResolvedValueOnce({
        errorCode: 0,
        throttleTime: 0,
        apiVersions: [{ apiKey: API_KEYS.Metadata, minVersion: 0, maxVersion: 6 }],
      });

      const broker = new Broker({ connectionPool: asConnectionPool(pool), logger: silentLogger });
      const versions = await broker.apiVersions();

      expect(pool.send).toHaveBeenCalledTimes(1);
      expect(versions).toEqual({ [API_KEYS.Metadata]: { minVersion: 0, maxVersion: 6 } });
    });

    it('falls back to lower versions on UNSUPPORTED_VERSION', async () => {
      const pool = createFakeConnectionPool();
      pool.send
        .mockRejectedValueOnce(new KafkaProtocolError({ message: 'nope', type: 'UNSUPPORTED_VERSION', code: 35 }))
        .mockResolvedValueOnce({ errorCode: 0, throttleTime: 0, apiVersions: [] });

      const broker = new Broker({ connectionPool: asConnectionPool(pool), logger: silentLogger });
      const versions = await broker.apiVersions();

      expect(pool.send).toHaveBeenCalledTimes(2);
      expect(versions).toEqual({});
    });

    it('rethrows any other error immediately', async () => {
      const pool = createFakeConnectionPool();
      pool.send.mockRejectedValueOnce(new Error('connection reset'));

      const broker = new Broker({ connectionPool: asConnectionPool(pool), logger: silentLogger });
      await expect(broker.apiVersions()).rejects.toThrow('connection reset');
      expect(pool.send).toHaveBeenCalledTimes(1);
    });
  });

  describe('connect', () => {
    it('fetches api versions, sets them on the pool, and authenticates', async () => {
      const pool = createFakeConnectionPool();
      pool.send.mockResolvedValueOnce({
        errorCode: 0,
        throttleTime: 0,
        apiVersions: [{ apiKey: API_KEYS.SaslAuthenticate, minVersion: 0, maxVersion: 1 }],
      });

      const broker = new Broker({ connectionPool: asConnectionPool(pool), logger: silentLogger });
      await broker.connect();

      expect(pool.setVersions).toHaveBeenCalledWith(broker.versions);
      expect(pool.setSupportAuthenticationProtocol).toHaveBeenCalledWith(true);
    });

    it('does not re-fetch api versions when already provided', async () => {
      const pool = createFakeConnectionPool();
      const broker = new Broker({
        connectionPool: asConnectionPool(pool),
        logger: silentLogger,
        versions: { [API_KEYS.SaslAuthenticate]: { maxVersion: 1 } },
      });

      await broker.connect();

      expect(pool.send).not.toHaveBeenCalled();
      expect(pool.setVersions).toHaveBeenCalledWith(broker.versions);
    });

    it('is a no-op if already connected', async () => {
      const pool = createFakeConnectionPool({ isConnected: vi.fn().mockReturnValue(true) });
      const broker = new Broker({ connectionPool: asConnectionPool(pool), logger: silentLogger });

      await broker.connect();

      expect(pool.getConnection).not.toHaveBeenCalled();
    });
  });

  describe('disconnect', () => {
    it('destroys the connection pool', async () => {
      const pool = createFakeConnectionPool();
      const broker = new Broker({ connectionPool: asConnectionPool(pool), logger: silentLogger });
      await broker.disconnect();
      expect(pool.destroy).toHaveBeenCalledOnce();
    });
  });

  describe('request dispatch', () => {
    async function connectedBroker(pool: FakeConnectionPool): Promise<Broker> {
      const allApiKeys = Object.values(API_KEYS).map((apiKey) => ({ apiKey, minVersion: 0, maxVersion: 99 }));
      pool.send.mockResolvedValueOnce({ errorCode: 0, throttleTime: 0, apiVersions: allApiKeys });
      const broker = new Broker({ connectionPool: asConnectionPool(pool), logger: silentLogger });
      await broker.connect();
      pool.send.mockClear();
      return broker;
    }

    it('heartbeat sends through the negotiated version and returns the parsed response', async () => {
      const pool = createFakeConnectionPool();
      const broker = await connectedBroker(pool);
      pool.send.mockResolvedValueOnce({ throttleTime: 0 });

      const result = await broker.heartbeat({ groupId: 'g', groupGenerationId: 1, memberId: 'm' });

      expect(result).toEqual({ throttleTime: 0 });
      expect(pool.send).toHaveBeenCalledOnce();
      const [sent] = pool.send.mock.calls[0] as [{ request: { apiKey: number } }];
      expect(sent.request.apiKey).toBe(API_KEYS.Heartbeat);
    });

    it('joinGroup retries with the assigned memberId on KafkaMemberIdRequired', async () => {
      const pool = createFakeConnectionPool();
      const broker = await connectedBroker(pool);

      pool.send
        .mockRejectedValueOnce(new KafkaMemberIdRequired({ message: 'need a member id' }, { memberId: 'assigned-1' }))
        .mockResolvedValueOnce({ memberId: 'assigned-1', leader: 'assigned-1', members: [] });

      const result = await broker.joinGroup({
        groupId: 'g',
        sessionTimeout: 10_000,
        memberId: '',
        protocolType: 'consumer',
        groupProtocols: [],
      });

      expect(pool.send).toHaveBeenCalledTimes(2);
      expect(result).toMatchObject({ memberId: 'assigned-1' });
    });

    it('reconnects when the connection pool reports a closed connection', async () => {
      const pool = createFakeConnectionPool();
      const broker = await connectedBroker(pool);
      pool.send.mockRejectedValueOnce(new KafkaConnectionClosedError('closed'));

      await expect(broker.heartbeat({ groupId: 'g', groupGenerationId: 1, memberId: 'm' })).rejects.toThrow(
        KafkaConnectionClosedError,
      );
      expect(pool.destroy).toHaveBeenCalledOnce();
    });

    it('selects Produce v2 when the broker only advertises MessageSet versions', async () => {
      const pool = createFakeConnectionPool();
      const broker = new Broker({
        connectionPool: asConnectionPool(pool),
        logger: silentLogger,
        versions: { [API_KEYS.Produce]: { minVersion: 0, maxVersion: 2 } },
      });

      await broker.connect();
      pool.send.mockResolvedValueOnce({ topics: [], throttleTime: 0 });

      await broker.produce({
        acks: 1,
        timeout: 1000,
        topicData: [{ topic: 't', partitions: [{ partition: 0, messages: [{ value: 'v' }] }] }],
      });
      expect(pool.send).toHaveBeenCalledOnce();
    });

    it('listGroups sends an empty options object', async () => {
      const pool = createFakeConnectionPool();
      const broker = await connectedBroker(pool);
      pool.send.mockResolvedValueOnce({ throttleTime: 0, groups: [] });

      const result = await broker.listGroups();
      expect(result).toEqual({ throttleTime: 0, groups: [] });
    });

    it('normalizes ListOffsets v0 offsets[] to a single offset bigint', async () => {
      const pool = createFakeConnectionPool();
      const broker = new Broker({
        connectionPool: asConnectionPool(pool),
        logger: silentLogger,
        versions: { [API_KEYS.ListOffsets]: { minVersion: 0, maxVersion: 0 } },
      });
      await broker.connect();
      pool.send.mockResolvedValueOnce({
        responses: [{ topic: 't', partitions: [{ partition: 0, errorCode: 0, offsets: [1n, 2n, 3n] }] }],
      });

      const result = await broker.listOffsets({ topics: [{ topic: 't', partitions: [{ partition: 0 }] }] });

      expect(result.responses[0]?.partitions[0]).toEqual({
        partition: 0,
        errorCode: 0,
        timestamp: -1n,
        offset: 3n,
      });
    });
  });
});
