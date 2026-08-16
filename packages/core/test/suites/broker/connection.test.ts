import { afterEach, describe, expect, it } from 'vitest';
import { Broker } from '../../../src/broker/index.js';
import {
  createConnectionPool,
  connectionOpts,
  newLogger,
  saslEntries,
  saslSCRAM256ConnectionOpts,
  sslConnectionOpts,
} from '../../helpers/index.js';

describe('broker.connection', () => {
  let broker: Broker | undefined;

  afterEach(async () => {
    await broker?.disconnect();
  });

  it('establishes a PLAINTEXT connection and loads api versions', async () => {
    broker = new Broker({ connectionPool: createConnectionPool(connectionOpts()), logger: newLogger() });
    expect(broker.isConnected()).toBe(false);
    expect(broker.versions).toBeNull();
    await broker.connect();
    expect(broker.isConnected()).toBe(true);
    expect(broker.versions).toBeTruthy();
  });

  it('rejects an unsupported SASL mechanism', async () => {
    broker = new Broker({
      connectionPool: createConnectionPool(
        sslConnectionOpts({
          port: 9094,
          sasl: {
            mechanism: 'fake-mechanism',
            authenticationProvider: () => ({
              authenticate: async () => {
                throw new Error('unsupported');
              },
            }),
          },
        }),
      ),
      logger: newLogger(),
    });
    await expect(broker.connect()).rejects.toThrow(/The broker does not support the requested SASL mechanism/);
  });

  it.each(saslEntries)('authenticates with SASL $name', async (entry) => {
    broker = new Broker({ connectionPool: createConnectionPool(entry.opts()), logger: newLogger() });
    expect(broker.isConnected()).toBe(false);
    await broker.connect();
    expect(broker.isConnected()).toBe(true);
  });

  it('handles parallel SCRAM connect calls', async () => {
    broker = new Broker({
      connectionPool: createConnectionPool(saslSCRAM256ConnectionOpts()),
      logger: newLogger(),
    });
    await Promise.all([broker.connect(), broker.connect(), broker.connect()]);
    expect(broker.isConnected()).toBe(true);
  });

  it('reports disconnected before SASL authentication completes', async () => {
    const connectionPool = createConnectionPool(saslEntries[0]!.opts());
    broker = new Broker({ connectionPool, logger: newLogger() });
    await connectionPool.getConnection();
    expect(broker.isConnected()).toBe(false);
  });
});
