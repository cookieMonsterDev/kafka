import { describe, expect, it, vi } from 'vitest';
import { createLogger, LOG_LEVELS } from '../loggers/index';
import { ConnectionPool } from '../network/connection-pool';
import { createDefaultSocketFactory } from '../network/socket-factory';
import { connectionPoolBuilder } from './connection-pool-builder';

const silentLogger = createLogger({ level: LOG_LEVELS.NOTHING, logCreator: () => () => {} });

const baseOptions = {
  socketFactory: createDefaultSocketFactory(),
  clientId: 'test-client',
  requestTimeout: 1000,
  connectionTimeout: 1000,
  logger: silentLogger,
};

describe('cluster/connectionPoolBuilder', () => {
  it('builds a ConnectionPool for an explicit destination without consulting the broker list', async () => {
    const builder = connectionPoolBuilder({ ...baseOptions, brokers: [] });
    const pool = await builder.build({ host: 'explicit-host', port: 9099 });

    expect(pool).toBeInstanceOf(ConnectionPool);
    expect(pool.host).toBe('explicit-host');
    expect(pool.port).toBe(9099);
    await pool.destroy();
  });

  it('picks a broker from a static list and parses host:port', async () => {
    const builder = connectionPoolBuilder({ ...baseOptions, brokers: ['broker-1:9092'] });
    const pool = await builder.build();

    expect(pool.host).toBe('broker-1');
    expect(pool.port).toBe(9092);
    await pool.destroy();
  });

  it('rotates through the broker list on successive calls', async () => {
    const builder = connectionPoolBuilder({ ...baseOptions, brokers: ['broker-1:9092', 'broker-2:9093'] });

    const first = await builder.build();
    const second = await builder.build();
    const third = await builder.build();

    expect([first.host, second.host, third.host]).toEqual(['broker-1', 'broker-2', 'broker-1']);
    await Promise.all([first.destroy(), second.destroy(), third.destroy()]);
  });

  it('parses bracketed IPv6 bootstrap addresses', async () => {
    const builder = connectionPoolBuilder({ ...baseOptions, brokers: ['[::1]:9092'] });
    const pool = await builder.build();

    expect(pool.host).toBe('::1');
    expect(pool.port).toBe(9092);
    await pool.destroy();
  });

  it('throws a clear error for a broker string with no port', async () => {
    const builder = connectionPoolBuilder({ ...baseOptions, brokers: ['localhost'] });
    await expect(builder.build()).rejects.toThrow('missing a port');
  });

  it('resolves a function-based broker list', async () => {
    const brokers = vi.fn().mockResolvedValue(['dynamic-broker:9092']);
    const builder = connectionPoolBuilder({ ...baseOptions, brokers });
    const pool = await builder.build();

    expect(brokers).toHaveBeenCalledOnce();
    expect(pool.host).toBe('dynamic-broker');
    await pool.destroy();
  });

  it('wraps an error thrown by a function-based broker list', async () => {
    const brokers = vi.fn().mockRejectedValue(new Error('DNS failure'));
    const builder = connectionPoolBuilder({ ...baseOptions, brokers });

    await expect(builder.build()).rejects.toThrow('"config.brokers" threw: DNS failure');
  });

  it('throws for a null broker list', async () => {
    const builder = connectionPoolBuilder({ ...baseOptions, brokers: null as never });
    await expect(builder.build()).rejects.toThrow('brokers should not be null');
  });

  it('throws for an empty broker array', async () => {
    const builder = connectionPoolBuilder({ ...baseOptions, brokers: [] });
    await expect(builder.build()).rejects.toThrow('brokers array is empty');
  });

  it('throws for an invalid broker entry', async () => {
    const builder = connectionPoolBuilder({ ...baseOptions, brokers: ['valid:9092', 42 as never] });
    await expect(builder.build()).rejects.toThrow('broker at index 1 is invalid');
  });

  it('threads sasl config through to the built pool', async () => {
    const sasl = { mechanism: 'plain', username: 'user', password: 'pw' };
    const builder = connectionPoolBuilder({ ...baseOptions, brokers: ['broker:9092'], sasl });
    const pool = await builder.build();

    expect(pool.sasl).toBe(sasl);
    await pool.destroy();
  });
});
