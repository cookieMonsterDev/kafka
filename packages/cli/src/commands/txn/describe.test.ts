import { describe, expect, it, vi } from 'vitest';
import { createFakeAdmin } from '../../testing/create-fake-admin';
import { createFakeCommandContext } from '../../testing/create-command-context';
import { txnDescribeCommand } from './describe';

function fakeState(transactionalId: string) {
  return {
    errorCode: 0,
    transactionalId,
    transactionState: 'Ongoing',
    transactionTimeoutMs: 60000,
    transactionStartTimeMs: 123n,
    producerId: 1000n,
    producerEpoch: 3,
    topics: [],
  };
}

describe('txnDescribeCommand', () => {
  it('requires at least one transactional id', async () => {
    const { context } = createFakeCommandContext({ flags: { brokers: 'localhost:9092' }, positionals: [] });
    await expect(txnDescribeCommand.run(context)).rejects.toThrow(/at least one transactional id/);
  });

  it('describes a single transactional id with one call', async () => {
    const describeTransactions = vi.fn(async () => ({ transactionStates: [fakeState('orders-1')] }));
    const admin = createFakeAdmin({ describeTransactions, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      positionals: ['orders-1'],
      openAdmin: async () => admin,
    });

    const code = await txnDescribeCommand.run(context);

    expect(code).toBe(0);
    expect(describeTransactions).toHaveBeenCalledWith(['orders-1']);
  });

  it('fans out one call per id when more than one transactional id is given', async () => {
    const describeTransactions = vi.fn(async (ids: string[]) => ({ transactionStates: [fakeState(ids[0]!)] }));
    const admin = createFakeAdmin({ describeTransactions, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      positionals: ['orders-1', 'orders-2'],
      openAdmin: async () => admin,
    });

    const code = await txnDescribeCommand.run(context);

    expect(code).toBe(0);
    expect(describeTransactions).toHaveBeenCalledTimes(2);
    expect(describeTransactions).toHaveBeenCalledWith(['orders-1']);
    expect(describeTransactions).toHaveBeenCalledWith(['orders-2']);
  });

  it('returns exit 4 on a fanned-out partial failure — a nonexistent id throws rather than returning an error row', async () => {
    const describeTransactions = vi.fn(async (ids: string[]) => {
      if (ids[0] === 'ghost') throw new Error('TRANSACTIONAL_ID_NOT_FOUND');
      return { transactionStates: [fakeState(ids[0]!)] };
    });
    const admin = createFakeAdmin({ describeTransactions, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      positionals: ['orders-1', 'ghost'],
      openAdmin: async () => admin,
    });

    const code = await txnDescribeCommand.run(context);
    expect(code).toBe(4);
  });

  it('returns exit 1 when every fanned-out call fails', async () => {
    const describeTransactions = vi.fn(async () => {
      throw new Error('boom');
    });
    const admin = createFakeAdmin({ describeTransactions, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      positionals: ['orders-1', 'orders-2'],
      openAdmin: async () => admin,
    });

    const code = await txnDescribeCommand.run(context);
    expect(code).toBe(1);
  });

  it('renders producer id, epoch, and timeout for a healthy transaction', async () => {
    const admin = createFakeAdmin({
      describeTransactions: async () => ({ transactionStates: [fakeState('orders-1')] }),
      disconnect: async () => {},
    });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      positionals: ['orders-1'],
      openAdmin: async () => admin,
    });

    await txnDescribeCommand.run(context);
    const written = stdoutWrite.mock.calls[0]![0];
    expect(written).toContain('orders-1');
    expect(written).toContain('Ongoing');
    expect(written).toContain('1000');
    expect(written).toContain('3');
    expect(written).toContain('60000');
  });

  it('reports json output with the resolved transaction state', async () => {
    const admin = createFakeAdmin({
      describeTransactions: async () => ({ transactionStates: [fakeState('orders-1')] }),
      disconnect: async () => {},
    });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      positionals: ['orders-1'],
      openAdmin: async () => admin,
      format: 'json',
    });

    await txnDescribeCommand.run(context);
    const written = JSON.parse(stdoutWrite.mock.calls[0]![0]) as {
      transactionStates: { transactionalId: string; ok: boolean; producerId: string }[];
    };
    expect(written.transactionStates[0]!.transactionalId).toBe('orders-1');
    expect(written.transactionStates[0]!.ok).toBe(true);
    expect(written.transactionStates[0]!.producerId).toBe('1000');
  });

  it('treats a missing result as a failure for that id', async () => {
    const admin = createFakeAdmin({
      describeTransactions: async () => ({ transactionStates: [] }),
      disconnect: async () => {},
    });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      positionals: ['orders-1'],
      openAdmin: async () => admin,
    });

    const code = await txnDescribeCommand.run(context);
    expect(code).toBe(1);
  });

  it('disconnects even when a single-id call throws', async () => {
    const disconnect = vi.fn(async () => {});
    const admin = createFakeAdmin({
      describeTransactions: async () => {
        throw new Error('boom');
      },
      disconnect,
    });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      positionals: ['orders-1'],
      openAdmin: async () => admin,
    });

    await expect(txnDescribeCommand.run(context)).rejects.toThrow('boom');
    expect(disconnect).toHaveBeenCalledTimes(1);
  });
});
