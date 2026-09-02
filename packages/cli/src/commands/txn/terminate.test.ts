import { describe, expect, it, vi } from 'vitest';
import { CliAbortedError } from '../../errors/aborted-error';
import { createFakeAdmin } from '../../testing/create-fake-admin';
import { createFakeCommandContext, EMPTY_RESOLVED_CLI_CONFIG } from '../../testing/create-command-context';
import { txnTerminateCommand } from './terminate';

describe('txnTerminateCommand', () => {
  it('requires a transactional id', async () => {
    const { context } = createFakeCommandContext({ flags: { brokers: 'localhost:9092', yes: true }, positionals: [] });
    await expect(txnTerminateCommand.run(context)).rejects.toThrow(/requires a transactional id/);
  });

  it('aborts without --yes off a TTY', async () => {
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      positionals: ['orders-1'],
    });
    await expect(txnTerminateCommand.run(context)).rejects.toThrow(CliAbortedError);
  });

  it('skips confirmation when cli.confirmDestructive is false', async () => {
    const forceTerminateTransaction = vi.fn(async () => ({ transactionalId: 'orders-1', errorCode: 0 }));
    const admin = createFakeAdmin({ forceTerminateTransaction, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      positionals: ['orders-1'],
      openAdmin: async () => admin,
      config: { ...EMPTY_RESOLVED_CLI_CONFIG, cli: { confirmDestructive: false } },
    });

    expect(await txnTerminateCommand.run(context)).toBe(0);
  });

  it('terminates the given transactional id with the given timeout', async () => {
    const forceTerminateTransaction = vi.fn(async () => ({
      transactionalId: 'orders-1',
      errorCode: 0,
      producerId: 1000n,
      producerEpoch: 4,
    }));
    const admin = createFakeAdmin({ forceTerminateTransaction, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', yes: true, timeout: 5000 },
      positionals: ['orders-1'],
      openAdmin: async () => admin,
    });

    const code = await txnTerminateCommand.run(context);

    expect(code).toBe(0);
    expect(forceTerminateTransaction).toHaveBeenCalledWith({ transactionalId: 'orders-1', transactionTimeout: 5000 });
  });

  it('returns exit 1 when the termination fails', async () => {
    const admin = createFakeAdmin({
      forceTerminateTransaction: async () => ({ transactionalId: 'orders-1', errorCode: 32 }),
      disconnect: async () => {},
    });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', yes: true },
      positionals: ['orders-1'],
      openAdmin: async () => admin,
    });

    expect(await txnTerminateCommand.run(context)).toBe(1);
  });

  it('disconnects even when forceTerminateTransaction throws', async () => {
    const disconnect = vi.fn(async () => {});
    const admin = createFakeAdmin({
      forceTerminateTransaction: async () => {
        throw new Error('boom');
      },
      disconnect,
    });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', yes: true },
      positionals: ['orders-1'],
      openAdmin: async () => admin,
    });

    await expect(txnTerminateCommand.run(context)).rejects.toThrow('boom');
    expect(disconnect).toHaveBeenCalledTimes(1);
  });
});
