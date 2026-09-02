import { describe, expect, it, vi } from 'vitest';
import { CliUsageError } from '../../args/coerce';
import { CliAbortedError } from '../../errors/aborted-error';
import { createFakeAdmin } from '../../testing/create-fake-admin';
import { createFakeCommandContext, EMPTY_RESOLVED_CLI_CONFIG } from '../../testing/create-command-context';
import { clusterUnregisterBrokerCommand } from './unregister-broker';

describe('clusterUnregisterBrokerCommand', () => {
  it('requires --broker-id', async () => {
    const openAdmin = vi.fn();
    const { context } = createFakeCommandContext({ flags: { brokers: 'localhost:9092', yes: true }, openAdmin });

    await expect(clusterUnregisterBrokerCommand.run(context)).rejects.toThrow(/requires --broker-id/);
    expect(openAdmin).not.toHaveBeenCalled();
  });

  it('aborts without --yes off a TTY', async () => {
    const { context } = createFakeCommandContext({ flags: { brokers: 'localhost:9092', 'broker-id': 3 } });
    await expect(clusterUnregisterBrokerCommand.run(context)).rejects.toThrow(CliAbortedError);
  });

  it('skips confirmation when cli.confirmDestructive is false', async () => {
    const unregisterBroker = vi.fn(async () => {});
    const admin = createFakeAdmin({ unregisterBroker, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', 'broker-id': 3 },
      openAdmin: async () => admin,
      config: { ...EMPTY_RESOLVED_CLI_CONFIG, cli: { confirmDestructive: false } },
    });

    const code = await clusterUnregisterBrokerCommand.run(context);
    expect(code).toBe(0);
  });

  it('unregisters the broker and reports success', async () => {
    const unregisterBroker = vi.fn(async () => {});
    const admin = createFakeAdmin({ unregisterBroker, disconnect: async () => {} });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', 'broker-id': 3, yes: true },
      openAdmin: async () => admin,
    });

    const code = await clusterUnregisterBrokerCommand.run(context);

    expect(code).toBe(0);
    expect(unregisterBroker).toHaveBeenCalledWith({ brokerId: 3 });
    expect(stdoutWrite.mock.calls[0]![0]).toContain('Broker 3 unregistered');
  });

  it('reports json output', async () => {
    const unregisterBroker = vi.fn(async () => {});
    const admin = createFakeAdmin({ unregisterBroker, disconnect: async () => {} });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', 'broker-id': 3, yes: true },
      openAdmin: async () => admin,
      format: 'json',
    });

    await clusterUnregisterBrokerCommand.run(context);

    expect(JSON.parse(stdoutWrite.mock.calls[0]![0])).toEqual({ brokerId: 3, ok: true });
  });

  it('propagates a failure from unregisterBroker', async () => {
    const admin = createFakeAdmin({
      unregisterBroker: async () => {
        throw new Error('boom');
      },
      disconnect: async () => {},
    });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', 'broker-id': 3, yes: true },
      openAdmin: async () => admin,
    });

    await expect(clusterUnregisterBrokerCommand.run(context)).rejects.toThrow('boom');
  });

  it('disconnects even when unregisterBroker throws', async () => {
    const disconnect = vi.fn(async () => {});
    const admin = createFakeAdmin({
      unregisterBroker: async () => {
        throw new Error('boom');
      },
      disconnect,
    });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', 'broker-id': 3, yes: true },
      openAdmin: async () => admin,
    });

    await expect(clusterUnregisterBrokerCommand.run(context)).rejects.toThrow('boom');
    expect(disconnect).toHaveBeenCalledTimes(1);
  });

  it('rejects an unrecognized --broker-id type before opening an admin', async () => {
    const openAdmin = vi.fn();
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', 'broker-id': undefined, yes: true },
      openAdmin,
    });

    await expect(clusterUnregisterBrokerCommand.run(context)).rejects.toThrow(CliUsageError);
    expect(openAdmin).not.toHaveBeenCalled();
  });
});
