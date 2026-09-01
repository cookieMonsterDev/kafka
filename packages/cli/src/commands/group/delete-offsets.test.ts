import { describe, expect, it, vi } from 'vitest';
import { CliAbortedError } from '../../errors/aborted-error';
import { createFakeAdmin } from '../../testing/create-fake-admin';
import { createFakeCommandContext } from '../../testing/create-command-context';
import { groupDeleteOffsetsCommand } from './delete-offsets';

describe('groupDeleteOffsetsCommand', () => {
  it('requires a group id', async () => {
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', yes: true, topic: ['orders'] },
      positionals: [],
    });
    await expect(groupDeleteOffsetsCommand.run(context)).rejects.toThrow(/requires a group id/);
  });

  it('requires at least one --topic', async () => {
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', yes: true },
      positionals: ['my-group'],
    });
    await expect(groupDeleteOffsetsCommand.run(context)).rejects.toThrow(/--topic/);
  });

  it('aborts without --yes off a TTY', async () => {
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', topic: ['orders'] },
      positionals: ['my-group'],
    });
    await expect(groupDeleteOffsetsCommand.run(context)).rejects.toThrow(CliAbortedError);
  });

  it('applies explicit --partition to the single --topic without discovering partitions', async () => {
    const deleteGroupOffsets = vi.fn(async () => ({ topics: [] }));
    const fetchTopicOffsets = vi.fn(async () => []);
    const admin = createFakeAdmin({ deleteGroupOffsets, fetchTopicOffsets, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', yes: true, topic: ['orders'], partition: [0, 1] },
      positionals: ['my-group'],
      openAdmin: async () => admin,
    });

    const code = await groupDeleteOffsetsCommand.run(context);

    expect(code).toBe(0);
    expect(fetchTopicOffsets).not.toHaveBeenCalled();
    expect(deleteGroupOffsets).toHaveBeenCalledWith({
      groupId: 'my-group',
      topics: [{ topic: 'orders', partitions: [0, 1] }],
    });
  });

  it('discovers partitions via fetchTopicOffsets when --partition is omitted', async () => {
    const deleteGroupOffsets = vi.fn(async () => ({ topics: [] }));
    const fetchTopicOffsets = vi.fn(async () => [
      { partition: 0, offset: 1n, high: 1n, low: 0n },
      { partition: 1, offset: 1n, high: 1n, low: 0n },
    ]);
    const admin = createFakeAdmin({ deleteGroupOffsets, fetchTopicOffsets, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', yes: true, topic: ['orders'] },
      positionals: ['my-group'],
      openAdmin: async () => admin,
    });

    const code = await groupDeleteOffsetsCommand.run(context);

    expect(code).toBe(0);
    expect(fetchTopicOffsets).toHaveBeenCalledWith('orders');
    expect(deleteGroupOffsets).toHaveBeenCalledWith({
      groupId: 'my-group',
      topics: [{ topic: 'orders', partitions: [0, 1] }],
    });
  });

  it('fans out one deleteGroupOffsets call per --topic, applying --partition to every topic', async () => {
    const deleteGroupOffsets = vi.fn(async () => ({ topics: [] }));
    const admin = createFakeAdmin({ deleteGroupOffsets, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', yes: true, topic: ['orders', 'payments'], partition: [0] },
      positionals: ['my-group'],
      openAdmin: async () => admin,
    });

    const code = await groupDeleteOffsetsCommand.run(context);

    expect(code).toBe(0);
    expect(deleteGroupOffsets).toHaveBeenCalledTimes(2);
    expect(deleteGroupOffsets).toHaveBeenCalledWith({
      groupId: 'my-group',
      topics: [{ topic: 'orders', partitions: [0] }],
    });
    expect(deleteGroupOffsets).toHaveBeenCalledWith({
      groupId: 'my-group',
      topics: [{ topic: 'payments', partitions: [0] }],
    });
  });

  it('returns exit 4 on a fanned-out partial failure', async () => {
    const deleteGroupOffsets = vi.fn(async ({ topics }: { topics: { topic: string }[] }) => {
      if (topics[0]!.topic === 'payments') throw new Error('boom');
      return { topics: [] };
    });
    const admin = createFakeAdmin({ deleteGroupOffsets, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', yes: true, topic: ['orders', 'payments'], partition: [0] },
      positionals: ['my-group'],
      openAdmin: async () => admin,
    });

    const code = await groupDeleteOffsetsCommand.run(context);
    expect(code).toBe(4);
  });

  it('returns exit 1 when every fanned-out call fails', async () => {
    const deleteGroupOffsets = vi.fn(async () => {
      throw new Error('boom');
    });
    const admin = createFakeAdmin({ deleteGroupOffsets, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', yes: true, topic: ['orders', 'payments'], partition: [0] },
      positionals: ['my-group'],
      openAdmin: async () => admin,
    });

    const code = await groupDeleteOffsetsCommand.run(context);
    expect(code).toBe(1);
  });

  it('disconnects even when a single-topic call throws', async () => {
    const disconnect = vi.fn(async () => {});
    const admin = createFakeAdmin({
      deleteGroupOffsets: async () => {
        throw new Error('boom');
      },
      disconnect,
    });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', yes: true, topic: ['orders'], partition: [0] },
      positionals: ['my-group'],
      openAdmin: async () => admin,
    });

    await expect(groupDeleteOffsetsCommand.run(context)).rejects.toThrow('boom');
    expect(disconnect).toHaveBeenCalledTimes(1);
  });
});
