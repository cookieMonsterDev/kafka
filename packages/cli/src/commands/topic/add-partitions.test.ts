import { describe, expect, it, vi } from 'vitest';
import { createFakeAdmin } from '../../testing/create-fake-admin';
import { createFakeCommandContext } from '../../testing/create-command-context';
import { topicAddPartitionsCommand } from './add-partitions';

describe('topicAddPartitionsCommand', () => {
  it('raises a single topic to the given partition count', async () => {
    const createPartitions = vi.fn(async () => {});
    const admin = createFakeAdmin({ createPartitions, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', count: 6 },
      positionals: ['orders'],
      openAdmin: async () => admin,
    });

    const code = await topicAddPartitionsCommand.run(context);

    expect(code).toBe(0);
    expect(createPartitions).toHaveBeenCalledWith({
      topicPartitions: [{ topic: 'orders', count: 6 }],
      validateOnly: false,
    });
  });

  it('passes validateOnly: true for --dry-run', async () => {
    const createPartitions = vi.fn(async () => {});
    const admin = createFakeAdmin({ createPartitions, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', count: 6, 'dry-run': true },
      positionals: ['orders'],
      openAdmin: async () => admin,
    });

    await topicAddPartitionsCommand.run(context);
    expect(createPartitions).toHaveBeenCalledWith(expect.objectContaining({ validateOnly: true }));
  });

  it('requires --count', async () => {
    const { context } = createFakeCommandContext({ flags: { brokers: 'localhost:9092' }, positionals: ['orders'] });
    await expect(topicAddPartitionsCommand.run(context)).rejects.toThrow(/--count/);
  });

  it('requires at least one topic name', async () => {
    const { context } = createFakeCommandContext({ flags: { brokers: 'localhost:9092', count: 3 }, positionals: [] });
    await expect(topicAddPartitionsCommand.run(context)).rejects.toThrow(/at least one topic/);
  });

  it('fans out one call per topic when more than one topic is given', async () => {
    const createPartitions = vi.fn(async () => {});
    const admin = createFakeAdmin({ createPartitions, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', count: 6 },
      positionals: ['orders', 'payments'],
      openAdmin: async () => admin,
    });

    const code = await topicAddPartitionsCommand.run(context);

    expect(code).toBe(0);
    expect(createPartitions).toHaveBeenCalledTimes(2);
    expect(createPartitions).toHaveBeenCalledWith({
      topicPartitions: [{ topic: 'orders', count: 6 }],
      validateOnly: false,
    });
    expect(createPartitions).toHaveBeenCalledWith({
      topicPartitions: [{ topic: 'payments', count: 6 }],
      validateOnly: false,
    });
  });

  it('returns exit 4 on a fanned-out partial failure', async () => {
    const createPartitions = vi.fn(async ({ topicPartitions }: { topicPartitions: { topic: string }[] }) => {
      if (topicPartitions[0]?.topic === 'payments') throw new Error('boom');
    });
    const admin = createFakeAdmin({ createPartitions, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', count: 6 },
      positionals: ['orders', 'payments'],
      openAdmin: async () => admin,
    });

    const code = await topicAddPartitionsCommand.run(context);
    expect(code).toBe(4);
  });

  it('returns exit 1 when every fanned-out call fails', async () => {
    const createPartitions = vi.fn(async () => {
      throw new Error('boom');
    });
    const admin = createFakeAdmin({ createPartitions, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', count: 6 },
      positionals: ['orders', 'payments'],
      openAdmin: async () => admin,
    });

    const code = await topicAddPartitionsCommand.run(context);
    expect(code).toBe(1);
  });

  it('disconnects even when a single-topic call throws', async () => {
    const disconnect = vi.fn(async () => {});
    const admin = createFakeAdmin({
      createPartitions: async () => {
        throw new Error('boom');
      },
      disconnect,
    });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', count: 6 },
      positionals: ['orders'],
      openAdmin: async () => admin,
    });

    await expect(topicAddPartitionsCommand.run(context)).rejects.toThrow('boom');
    expect(disconnect).toHaveBeenCalledTimes(1);
  });
});
