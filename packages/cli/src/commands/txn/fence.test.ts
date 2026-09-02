import { describe, expect, it, vi } from 'vitest';
import { createFakeAdmin } from '../../testing/create-fake-admin';
import { createFakeCommandContext } from '../../testing/create-command-context';
import { txnFenceCommand } from './fence';

describe('txnFenceCommand', () => {
  it('requires at least one transactional id', async () => {
    const { context } = createFakeCommandContext({ flags: { brokers: 'localhost:9092' }, positionals: [] });
    await expect(txnFenceCommand.run(context)).rejects.toThrow(/at least one transactional id/);
  });

  it('fences every positional in a single call, with no timeout by default', async () => {
    const fenceProducers = vi.fn(async () => ({
      results: [{ transactionalId: 'orders-1', errorCode: 0, producerId: 1000n, producerEpoch: 1 }],
    }));
    const admin = createFakeAdmin({ fenceProducers, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      positionals: ['orders-1'],
      openAdmin: async () => admin,
    });

    const code = await txnFenceCommand.run(context);

    expect(code).toBe(0);
    expect(fenceProducers).toHaveBeenCalledWith({ transactionalIds: ['orders-1'], transactionTimeout: undefined });
  });

  it('passes --timeout through as transactionTimeout', async () => {
    const fenceProducers = vi.fn(async () => ({ results: [] }));
    const admin = createFakeAdmin({ fenceProducers, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', timeout: 30000 },
      positionals: ['orders-1'],
      openAdmin: async () => admin,
    });

    await txnFenceCommand.run(context);
    expect(fenceProducers).toHaveBeenCalledWith({ transactionalIds: ['orders-1'], transactionTimeout: 30000 });
  });

  it('derives a partial failure (exit 4) when one fence fails', async () => {
    const admin = createFakeAdmin({
      fenceProducers: async () => ({
        results: [
          { transactionalId: 'orders-1', errorCode: 0, producerId: 1000n, producerEpoch: 1 },
          { transactionalId: 'orders-2', errorCode: 32, producerId: undefined, producerEpoch: undefined },
        ],
      }),
      disconnect: async () => {},
    });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      positionals: ['orders-1', 'orders-2'],
      openAdmin: async () => admin,
    });

    expect(await txnFenceCommand.run(context)).toBe(4);
  });

  it('returns exit 1 when every fence fails', async () => {
    const admin = createFakeAdmin({
      fenceProducers: async () => ({ results: [{ transactionalId: 'orders-1', errorCode: 32 }] }),
      disconnect: async () => {},
    });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      positionals: ['orders-1'],
      openAdmin: async () => admin,
    });

    expect(await txnFenceCommand.run(context)).toBe(1);
  });

  it('renders "-" for producer id/epoch on a failed fence', async () => {
    const admin = createFakeAdmin({
      fenceProducers: async () => ({ results: [{ transactionalId: 'orders-1', errorCode: 32 }] }),
      disconnect: async () => {},
    });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      positionals: ['orders-1'],
      openAdmin: async () => admin,
    });

    await txnFenceCommand.run(context);
    const written = stdoutWrite.mock.calls[0]![0];
    expect(written).toContain('failed (code 32)');
  });

  it('disconnects even when fenceProducers throws', async () => {
    const disconnect = vi.fn(async () => {});
    const admin = createFakeAdmin({
      fenceProducers: async () => {
        throw new Error('boom');
      },
      disconnect,
    });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      positionals: ['orders-1'],
      openAdmin: async () => admin,
    });

    await expect(txnFenceCommand.run(context)).rejects.toThrow('boom');
    expect(disconnect).toHaveBeenCalledTimes(1);
  });
});
