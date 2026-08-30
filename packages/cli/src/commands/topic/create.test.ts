import { describe, expect, it, vi } from 'vitest';
import { createFakeAdmin } from '../../testing/create-fake-admin';
import { createFakeCommandContext } from '../../testing/create-command-context';
import { topicCreateCommand } from './create';

describe('topicCreateCommand', () => {
  it('creates a single topic with partitions and replication factor', async () => {
    const createTopics = vi.fn(async () => true);
    const admin = createFakeAdmin({ createTopics, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', partitions: 3, 'replication-factor': 2 },
      positionals: ['orders'],
      openAdmin: async () => admin,
    });

    const code = await topicCreateCommand.run(context);

    expect(code).toBe(0);
    expect(createTopics).toHaveBeenCalledWith({
      topics: [{ topic: 'orders', numPartitions: 3, replicationFactor: 2 }],
      validateOnly: false,
    });
  });

  it('passes validateOnly: true for --dry-run', async () => {
    const createTopics = vi.fn(async () => true);
    const admin = createFakeAdmin({ createTopics, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', 'dry-run': true },
      positionals: ['orders'],
      openAdmin: async () => admin,
    });

    await topicCreateCommand.run(context);
    expect(createTopics).toHaveBeenCalledWith(expect.objectContaining({ validateOnly: true }));
  });

  it('maps createTopics() === false without --if-not-exists to exit 1', async () => {
    const admin = createFakeAdmin({ createTopics: async () => false, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      positionals: ['orders'],
      openAdmin: async () => admin,
    });

    const code = await topicCreateCommand.run(context);
    expect(code).toBe(1);
  });

  it('maps createTopics() === false with --if-not-exists to exit 0', async () => {
    const admin = createFakeAdmin({ createTopics: async () => false, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', 'if-not-exists': true },
      positionals: ['orders'],
      openAdmin: async () => admin,
    });

    const code = await topicCreateCommand.run(context);
    expect(code).toBe(0);
  });

  it('fans out one call per topic when more than one topic is given', async () => {
    const createTopics = vi.fn(async () => true);
    const admin = createFakeAdmin({ createTopics, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      positionals: ['orders', 'payments'],
      openAdmin: async () => admin,
    });

    const code = await topicCreateCommand.run(context);

    expect(code).toBe(0);
    expect(createTopics).toHaveBeenCalledTimes(2);
    expect(createTopics).toHaveBeenCalledWith({ topics: [{ topic: 'orders' }], validateOnly: false });
    expect(createTopics).toHaveBeenCalledWith({ topics: [{ topic: 'payments' }], validateOnly: false });
  });

  it('reverts to one batched call with --fail-fast even for multiple topics', async () => {
    const createTopics = vi.fn(async () => true);
    const admin = createFakeAdmin({ createTopics, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', 'fail-fast': true },
      positionals: ['orders', 'payments'],
      openAdmin: async () => admin,
    });

    await topicCreateCommand.run(context);
    expect(createTopics).toHaveBeenCalledTimes(1);
    expect(createTopics).toHaveBeenCalledWith({
      topics: [{ topic: 'orders' }, { topic: 'payments' }],
      validateOnly: false,
    });
  });

  it('never falsely claims a specific topic "already exists" for a mixed --fail-fast batch', async () => {
    // core's createTopics() reports one boolean for the whole call: false only means "every
    // failing topic already existed", never which ones — a topic that really was created (e.g.
    // "orders" here) leaves no positive trace in that boolean. Regression for a bug where every
    // topic in the batch was reported as "already exists", even one that had just been created.
    const createTopics = vi.fn(async () => false);
    const admin = createFakeAdmin({ createTopics, disconnect: async () => {} });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', 'fail-fast': true },
      positionals: ['orders', 'payments'],
      openAdmin: async () => admin,
    });

    const code = await topicCreateCommand.run(context);

    expect(code).toBe(1);
    const written = stdoutWrite.mock.calls[0]?.[0] ?? '';
    expect(written).not.toContain('already exists');
    expect(written).toContain('per-topic detail is unavailable');
  });

  it('reports a mixed --fail-fast batch as fully ok when --if-not-exists is set', async () => {
    const createTopics = vi.fn(async () => false);
    const admin = createFakeAdmin({ createTopics, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', 'fail-fast': true, 'if-not-exists': true },
      positionals: ['orders', 'payments'],
      openAdmin: async () => admin,
    });

    const code = await topicCreateCommand.run(context);
    expect(code).toBe(0);
  });

  it('returns exit 4 on a fanned-out partial failure', async () => {
    const createTopics = vi.fn(async ({ topics }: { topics: { topic: string }[] }) => {
      if (topics[0]?.topic === 'orders') return true;
      throw new Error('already exists');
    });
    const admin = createFakeAdmin({ createTopics, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      positionals: ['orders', 'payments'],
      openAdmin: async () => admin,
    });

    const code = await topicCreateCommand.run(context);
    expect(code).toBe(4);
  });

  it('returns exit 1 when every fanned-out call fails', async () => {
    const createTopics = vi.fn(async () => {
      throw new Error('boom');
    });
    const admin = createFakeAdmin({ createTopics, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      positionals: ['orders', 'payments'],
      openAdmin: async () => admin,
    });

    const code = await topicCreateCommand.run(context);
    expect(code).toBe(1);
  });

  it('disconnects even when createTopics throws for a single topic', async () => {
    const disconnect = vi.fn(async () => {});
    const admin = createFakeAdmin({
      createTopics: async () => {
        throw new Error('boom');
      },
      disconnect,
    });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      positionals: ['orders'],
      openAdmin: async () => admin,
    });

    await expect(topicCreateCommand.run(context)).rejects.toThrow('boom');
    expect(disconnect).toHaveBeenCalledTimes(1);
  });

  it('requires at least one topic name', async () => {
    const { context } = createFakeCommandContext({ flags: { brokers: 'localhost:9092' }, positionals: [] });
    await expect(topicCreateCommand.run(context)).rejects.toThrow(/at least one topic/);
  });

  it('rejects combining --partitions with --replica-assignment', async () => {
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', partitions: 3, 'replica-assignment': ['0=1,2'] },
      positionals: ['orders'],
    });
    await expect(topicCreateCommand.run(context)).rejects.toThrow(/cannot be combined/);
  });
});
