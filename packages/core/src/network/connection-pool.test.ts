import net from 'node:net';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { createLogger, LOG_LEVELS } from '../loggers/index';
import { Encoder } from '../protocol/encoder';
import { API_KEYS } from '../protocol/requests/api-keys';
import { ConnectionPool } from './connection-pool';
import { createDefaultSocketFactory } from './socket-factory';

const silentLogger = createLogger({ level: LOG_LEVELS.NOTHING, logCreator: () => () => {} });

const requestWithApiKey = (apiKey: number) => ({
  apiKey,
  apiVersion: 0,
  apiName: 'x',
  encode: () => Promise.resolve(new Encoder()),
});

describe('network/ConnectionPool', () => {
  let servers: net.Server[] = [];
  let pools: ConnectionPool[] = [];

  afterEach(async () => {
    vi.useRealTimers();
    vi.restoreAllMocks();
    await Promise.all(pools.map((p) => p.destroy()));
    pools = [];
    await Promise.all(servers.map((s) => new Promise<void>((resolve) => s.close(() => resolve()))));
    servers = [];
  });

  const startServer = (): Promise<number> =>
    new Promise((resolve) => {
      const server = net.createServer();
      servers.push(server);
      server.listen(0, '127.0.0.1', () => resolve((server.address() as net.AddressInfo).port));
    });

  const createPool = (
    port: number,
    overrides: Partial<ConstructorParameters<typeof ConnectionPool>[0]> = {},
  ): ConnectionPool => {
    const pool = new ConnectionPool({
      host: '127.0.0.1',
      port,
      logger: silentLogger,
      socketFactory: createDefaultSocketFactory(),
      requestTimeout: 1000,
      connectionTimeout: 1000,
      ...overrides,
    });
    pools.push(pool);
    return pool;
  };

  it('creates a fixed pool of two connections', async () => {
    const port = await startServer();
    const pool = createPool(port);
    expect(pool.pool).toHaveLength(2);
  });

  it('connects lazily on getConnection', async () => {
    const port = await startServer();
    const pool = createPool(port);

    expect(pool.pool[0]!.isConnected()).toBe(false);
    const connection = await pool.getConnection(0);
    expect(connection).toBe(pool.pool[0]);
    expect(connection.isConnected()).toBe(true);
  });

  it('reuses an already-connected connection', async () => {
    const port = await startServer();
    const pool = createPool(port);

    const connectSpy = vi.spyOn(pool.pool[0]!, 'connect');
    await pool.getConnection(0);
    await pool.getConnection(0);

    expect(connectSpy).toHaveBeenCalledTimes(1);
  });

  it('routes Fetch requests to the dedicated second connection, everything else to the first', async () => {
    const port = await startServer();
    const pool = createPool(port);

    const fetchConnection = await pool.getConnectionByRequest({ request: requestWithApiKey(API_KEYS.Fetch) });
    expect(fetchConnection).toBe(pool.pool[1]);

    const metadataConnection = await pool.getConnectionByRequest({ request: requestWithApiKey(API_KEYS.Metadata) });
    expect(metadataConnection).toBe(pool.pool[0]);
  });

  it('isConnected reports true if any pooled connection is connected', async () => {
    const port = await startServer();
    const pool = createPool(port);

    expect(pool.isConnected()).toBe(false);
    await pool.getConnection(0);
    expect(pool.isConnected()).toBe(true);
    expect(pool.isAuthenticated()).toBe(false);
  });

  it('setSupportAuthenticationProtocol applies to every pooled connection', async () => {
    const port = await startServer();
    const pool = createPool(port);

    pool.setSupportAuthenticationProtocol(true);
    for (const connection of pool.pool) {
      expect(connection.getSupportAuthenticationProtocol()).toBe(true);
    }
  });

  it('setVersions does not throw when applied across the pool', async () => {
    const port = await startServer();
    const pool = createPool(port);

    expect(() => pool.setVersions({ [API_KEYS.Metadata]: { maxVersion: 6 } })).not.toThrow();
  });

  it('destroy disconnects every pooled connection', async () => {
    const port = await startServer();
    const pool = createPool(port);

    await pool.getConnection(0);
    await pool.getConnection(1);
    await pool.destroy();

    for (const connection of pool.pool) {
      expect(connection.isConnected()).toBe(false);
    }
  });

  it('waits reconnectBackoffMs after a failed connect before retrying', async () => {
    vi.useFakeTimers();
    const pool = createPool(1, {
      reconnectBackoffMs: 200,
      reconnectBackoffMaxMs: 1000,
      connectionTimeout: 50,
    });
    const connectSpy = vi
      .spyOn(pool.pool[0]!, 'connect')
      .mockRejectedValueOnce(new Error('boom'))
      .mockResolvedValue(true);

    await expect(pool.getConnection(0)).rejects.toThrow('boom');
    expect(connectSpy).toHaveBeenCalledTimes(1);

    const retry = pool.getConnection(0);
    await vi.advanceTimersByTimeAsync(199);
    expect(connectSpy).toHaveBeenCalledTimes(1);

    await vi.advanceTimersByTimeAsync(200);
    await retry;
    expect(connectSpy).toHaveBeenCalledTimes(2);
  });

  it('grows socket setup timeout after consecutive connect failures', async () => {
    vi.useFakeTimers();
    const pool = createPool(1, {
      connectionTimeout: 100,
      reconnectBackoffMs: 50,
      reconnectBackoffMaxMs: 10_000,
      socketConnectionSetupTimeoutMaxMs: 10_000,
    });
    const connectSpy = vi.spyOn(pool.pool[0]!, 'connect').mockImplementation(() => Promise.reject(new Error('boom')));

    await expect(pool.getConnection(0)).rejects.toThrow('boom');
    expect(connectSpy.mock.calls[0]?.[0]).toBe(100);

    const second = pool.getConnection(0).then(
      () => {
        throw new Error('expected connect to fail');
      },
      (error: unknown) => error,
    );
    await vi.advanceTimersByTimeAsync(50);
    await expect(second).resolves.toEqual(expect.objectContaining({ message: 'boom' }));
    expect(connectSpy.mock.calls[1]?.[0]).toBe(100);

    const third = pool.getConnection(0).then(
      () => {
        throw new Error('expected connect to fail');
      },
      (error: unknown) => error,
    );
    await vi.advanceTimersByTimeAsync(100);
    await expect(third).resolves.toEqual(expect.objectContaining({ message: 'boom' }));
    expect(connectSpy.mock.calls[2]?.[0]).toBe(200);
  });
});
