import { describe, expect, it, vi } from 'vitest';
import { CliAbortedError } from '../../errors/aborted-error';
import { createFakeAdmin } from '../../testing/create-fake-admin';
import { createFakeCommandContext, EMPTY_RESOLVED_CLI_CONFIG } from '../../testing/create-command-context';
import { clusterUnregisterControllerCommand } from './unregister-controller';

describe('clusterUnregisterControllerCommand', () => {
  it('requires --controller-id', async () => {
    const openAdmin = vi.fn();
    const { context } = createFakeCommandContext({ flags: { brokers: 'localhost:9092', yes: true }, openAdmin });

    await expect(clusterUnregisterControllerCommand.run(context)).rejects.toThrow(/requires --controller-id/);
    expect(openAdmin).not.toHaveBeenCalled();
  });

  it('aborts without --yes off a TTY', async () => {
    const openAdmin = vi.fn();
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', 'controller-id': 3001 },
      openAdmin,
    });

    await expect(clusterUnregisterControllerCommand.run(context)).rejects.toThrow(CliAbortedError);
    expect(openAdmin).not.toHaveBeenCalled();
  });

  it('skips confirmation when cli.confirmDestructive is false', async () => {
    const unregisterController = vi.fn(async () => {});
    const admin = createFakeAdmin({ unregisterController, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', 'controller-id': 3001 },
      openAdmin: async () => admin,
      config: { ...EMPTY_RESOLVED_CLI_CONFIG, cli: { confirmDestructive: false } },
    });

    const code = await clusterUnregisterControllerCommand.run(context);
    expect(code).toBe(0);
    expect(unregisterController).toHaveBeenCalledWith({ controllerId: 3001 });
  });

  it('unregisters the controller and reports success', async () => {
    const unregisterController = vi.fn(async () => {});
    const admin = createFakeAdmin({ unregisterController, disconnect: async () => {} });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', 'controller-id': 3001, yes: true },
      openAdmin: async () => admin,
    });

    const code = await clusterUnregisterControllerCommand.run(context);

    expect(code).toBe(0);
    expect(unregisterController).toHaveBeenCalledWith({ controllerId: 3001 });
    expect(stdoutWrite.mock.calls[0]![0]).toContain('Controller 3001 unregistered');
  });

  it('reports json output', async () => {
    const unregisterController = vi.fn(async () => {});
    const admin = createFakeAdmin({ unregisterController, disconnect: async () => {} });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', 'controller-id': 3001, yes: true },
      openAdmin: async () => admin,
      format: 'json',
    });

    await clusterUnregisterControllerCommand.run(context);

    expect(JSON.parse(stdoutWrite.mock.calls[0]![0])).toEqual({ controllerId: 3001, ok: true });
  });

  it('propagates a failure and still disconnects', async () => {
    const disconnect = vi.fn(async () => {});
    const admin = createFakeAdmin({
      unregisterController: async () => {
        throw new Error('The given controller ID was not registered');
      },
      disconnect,
    });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', 'controller-id': 3001, yes: true },
      openAdmin: async () => admin,
    });

    await expect(clusterUnregisterControllerCommand.run(context)).rejects.toThrow('not registered');
    expect(disconnect).toHaveBeenCalledTimes(1);
  });
});
