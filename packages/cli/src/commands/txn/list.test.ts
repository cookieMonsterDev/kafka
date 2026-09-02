import { describe, expect, it, vi } from 'vitest';
import { CliUsageError } from '../../args/coerce';
import { createFakeAdmin } from '../../testing/create-fake-admin';
import { createFakeCommandContext } from '../../testing/create-command-context';
import { txnListCommand } from './list';

describe('txnListCommand', () => {
  it('lists with no filters by default', async () => {
    const listTransactions = vi.fn(async () => ({ transactionStates: [] }));
    const admin = createFakeAdmin({ listTransactions, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      openAdmin: async () => admin,
    });

    const code = await txnListCommand.run(context);

    expect(code).toBe(0);
    expect(listTransactions).toHaveBeenCalledWith({
      stateFilters: undefined,
      producerIdFilters: undefined,
      durationFilter: undefined,
      transactionalIdPattern: undefined,
    });
  });

  it('parses --state-filter, --producer-id-filter, --duration-filter, and --transactional-id-pattern', async () => {
    const listTransactions = vi.fn(async () => ({ transactionStates: [] }));
    const admin = createFakeAdmin({ listTransactions, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: {
        brokers: 'localhost:9092',
        'state-filter': ['Ongoing', 'PrepareCommit'],
        'producer-id-filter': ['1000', '2000'],
        'duration-filter': '5000',
        'transactional-id-pattern': 'orders-*',
      },
      openAdmin: async () => admin,
    });

    await txnListCommand.run(context);

    expect(listTransactions).toHaveBeenCalledWith({
      stateFilters: ['Ongoing', 'PrepareCommit'],
      producerIdFilters: [1000n, 2000n],
      durationFilter: 5000n,
      transactionalIdPattern: 'orders-*',
    });
  });

  it('rejects a non-numeric --producer-id-filter', async () => {
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', 'producer-id-filter': ['not-a-number'] },
    });
    await expect(txnListCommand.run(context)).rejects.toThrow(CliUsageError);
  });

  it('renders "(no transactions)" when nothing matches', async () => {
    const admin = createFakeAdmin({
      listTransactions: async () => ({ transactionStates: [] }),
      disconnect: async () => {},
    });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      openAdmin: async () => admin,
    });

    await txnListCommand.run(context);
    expect(stdoutWrite.mock.calls[0]![0]).toContain('(no transactions)');
  });

  it('renders a table row per transaction', async () => {
    const admin = createFakeAdmin({
      listTransactions: async () => ({
        transactionStates: [{ transactionalId: 'orders-1', producerId: 1000n, transactionState: 'Ongoing' }],
      }),
      disconnect: async () => {},
    });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      openAdmin: async () => admin,
    });

    await txnListCommand.run(context);
    const written = stdoutWrite.mock.calls[0]![0];
    expect(written).toContain('orders-1');
    expect(written).toContain('1000');
    expect(written).toContain('Ongoing');
  });

  it('reports json output with bigint producer ids as strings', async () => {
    const admin = createFakeAdmin({
      listTransactions: async () => ({
        transactionStates: [{ transactionalId: 'orders-1', producerId: 1000n, transactionState: 'Ongoing' }],
      }),
      disconnect: async () => {},
    });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      openAdmin: async () => admin,
      format: 'json',
    });

    await txnListCommand.run(context);
    const written = JSON.parse(stdoutWrite.mock.calls[0]![0]) as { transactionStates: { producerId: string }[] };
    expect(written.transactionStates[0]!.producerId).toBe('1000');
  });

  it('disconnects even when listTransactions throws', async () => {
    const disconnect = vi.fn(async () => {});
    const admin = createFakeAdmin({
      listTransactions: async () => {
        throw new Error('boom');
      },
      disconnect,
    });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      openAdmin: async () => admin,
    });

    await expect(txnListCommand.run(context)).rejects.toThrow('boom');
    expect(disconnect).toHaveBeenCalledTimes(1);
  });
});
