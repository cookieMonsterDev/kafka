import { describe, expect, it, vi } from 'vitest';
import { CliAbortedError } from '../../errors/aborted-error';
import { CliUsageError } from '../../args/coerce';
import { createFakeAdmin } from '../../testing/create-fake-admin';
import { createFakeCommandContext, EMPTY_RESOLVED_CLI_CONFIG } from '../../testing/create-command-context';
import { txnAbortCommand } from './abort';

const BASE_FLAGS = {
  brokers: 'localhost:9092',
  topic: 'orders',
  partition: 0,
  'producer-id': '1000',
  'producer-epoch': 2,
};

describe('txnAbortCommand', () => {
  it('requires --topic', async () => {
    const { context } = createFakeCommandContext({ flags: { ...BASE_FLAGS, topic: undefined, yes: true } });
    await expect(txnAbortCommand.run(context)).rejects.toThrow(/--topic/);
  });

  it('requires --partition', async () => {
    const { context } = createFakeCommandContext({ flags: { ...BASE_FLAGS, partition: undefined, yes: true } });
    await expect(txnAbortCommand.run(context)).rejects.toThrow(/--partition/);
  });

  it('requires --producer-id', async () => {
    const { context } = createFakeCommandContext({ flags: { ...BASE_FLAGS, 'producer-id': undefined, yes: true } });
    await expect(txnAbortCommand.run(context)).rejects.toThrow(/--producer-id/);
  });

  it('requires --producer-epoch', async () => {
    const { context } = createFakeCommandContext({ flags: { ...BASE_FLAGS, 'producer-epoch': undefined, yes: true } });
    await expect(txnAbortCommand.run(context)).rejects.toThrow(/--producer-epoch/);
  });

  it('rejects a non-numeric --producer-id', async () => {
    const { context } = createFakeCommandContext({ flags: { ...BASE_FLAGS, 'producer-id': 'nope', yes: true } });
    await expect(txnAbortCommand.run(context)).rejects.toThrow(CliUsageError);
  });

  it('aborts without --yes off a TTY', async () => {
    const { context } = createFakeCommandContext({ flags: { ...BASE_FLAGS } });
    await expect(txnAbortCommand.run(context)).rejects.toThrow(CliAbortedError);
  });

  it('skips confirmation when cli.confirmDestructive is false', async () => {
    const abortTransaction = vi.fn(async () => undefined);
    const admin = createFakeAdmin({ abortTransaction, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { ...BASE_FLAGS },
      openAdmin: async () => admin,
      config: { ...EMPTY_RESOLVED_CLI_CONFIG, cli: { confirmDestructive: false } },
    });

    expect(await txnAbortCommand.run(context)).toBe(0);
  });

  it('calls abortTransaction with every parsed field', async () => {
    const abortTransaction = vi.fn(async () => undefined);
    const admin = createFakeAdmin({ abortTransaction, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { ...BASE_FLAGS, yes: true, 'coordinator-epoch': 5, 'transaction-version': 1 },
      openAdmin: async () => admin,
    });

    const code = await txnAbortCommand.run(context);

    expect(code).toBe(0);
    expect(abortTransaction).toHaveBeenCalledWith({
      topic: 'orders',
      partition: 0,
      producerId: 1000n,
      producerEpoch: 2,
      coordinatorEpoch: 5,
      transactionVersion: 1,
    });
  });

  it('leaves coordinatorEpoch/transactionVersion undefined when not given', async () => {
    const abortTransaction = vi.fn(async () => undefined);
    const admin = createFakeAdmin({ abortTransaction, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { ...BASE_FLAGS, yes: true },
      openAdmin: async () => admin,
    });

    await txnAbortCommand.run(context);
    expect(abortTransaction).toHaveBeenCalledWith(
      expect.objectContaining({ coordinatorEpoch: undefined, transactionVersion: undefined }),
    );
  });

  it('disconnects even when abortTransaction throws', async () => {
    const disconnect = vi.fn(async () => {});
    const admin = createFakeAdmin({
      abortTransaction: async () => {
        throw new Error('boom');
      },
      disconnect,
    });
    const { context } = createFakeCommandContext({ flags: { ...BASE_FLAGS, yes: true }, openAdmin: async () => admin });

    await expect(txnAbortCommand.run(context)).rejects.toThrow('boom');
    expect(disconnect).toHaveBeenCalledTimes(1);
  });
});
