import { describe, expect, it, vi } from 'vitest';
import { createFakeAdmin } from '../../testing/create-fake-admin';
import { createFakeCommandContext } from '../../testing/create-command-context';
import { txnDescribeCommand } from './describe';

describe('txnDescribeCommand', () => {
  it('requires at least one transactional id', async () => {
    const { context } = createFakeCommandContext({ flags: { brokers: 'localhost:9092' }, positionals: [] });
    await expect(txnDescribeCommand.run(context)).rejects.toThrow(/at least one transactional id/);
  });

  it('describes every positional in a single call', async () => {
    const describeTransactions = vi.fn(async () => ({ transactionStates: [] }));
    const admin = createFakeAdmin({ describeTransactions, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      positionals: ['orders-1', 'orders-2'],
      openAdmin: async () => admin,
    });

    const code = await txnDescribeCommand.run(context);

    expect(code).toBe(0);
    expect(describeTransactions).toHaveBeenCalledWith(['orders-1', 'orders-2']);
  });

  it('renders producer id, epoch, and timeout for a healthy transaction', async () => {
    const admin = createFakeAdmin({
      describeTransactions: async () => ({
        transactionStates: [
          {
            errorCode: 0,
            transactionalId: 'orders-1',
            transactionState: 'Ongoing',
            transactionTimeoutMs: 60000,
            transactionStartTimeMs: 123n,
            producerId: 1000n,
            producerEpoch: 3,
            topics: [],
          },
        ],
      }),
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

  it('surfaces a per-transaction error in the STATE column', async () => {
    const admin = createFakeAdmin({
      describeTransactions: async () => ({
        transactionStates: [
          {
            errorCode: 48,
            transactionalId: 'ghost',
            transactionState: 'Unknown',
            transactionTimeoutMs: 0,
            transactionStartTimeMs: -1n,
            producerId: -1n,
            producerEpoch: -1,
            topics: [],
          },
        ],
      }),
      disconnect: async () => {},
    });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      positionals: ['ghost'],
      openAdmin: async () => admin,
    });

    await txnDescribeCommand.run(context);
    expect(stdoutWrite.mock.calls[0]![0]).toContain('error (code 48)');
  });

  it('reports json output with the raw transactionStates shape', async () => {
    const transactionStates = [
      {
        errorCode: 0,
        transactionalId: 'orders-1',
        transactionState: 'Ongoing',
        transactionTimeoutMs: 60000,
        transactionStartTimeMs: 123n,
        producerId: 1000n,
        producerEpoch: 3,
        topics: [{ topic: 'orders', partitions: [0] }],
      },
    ];
    const admin = createFakeAdmin({
      describeTransactions: async () => ({ transactionStates }),
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
      transactionStates: { producerId: string; transactionalId: string }[];
    };
    expect(written.transactionStates[0]!.producerId).toBe('1000');
    expect(written.transactionStates[0]!.transactionalId).toBe('orders-1');
  });

  it('disconnects even when describeTransactions throws', async () => {
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
