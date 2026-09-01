import { describe, expect, it, vi } from 'vitest';
import { CliAbortedError } from '../../errors/aborted-error';
import { createFakeAdmin } from '../../testing/create-fake-admin';
import { createFakeCommandContext } from '../../testing/create-command-context';
import { groupResetOffsetsCommand } from './reset-offsets';

describe('groupResetOffsetsCommand', () => {
  it('requires a group id', async () => {
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', topic: ['orders'], to: 'earliest' },
      positionals: [],
    });
    await expect(groupResetOffsetsCommand.run(context)).rejects.toThrow(/requires a group id/);
  });

  it('requires at least one --topic', async () => {
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', to: 'earliest' },
      positionals: ['my-group'],
    });
    await expect(groupResetOffsetsCommand.run(context)).rejects.toThrow(/--topic/);
  });

  it('requires --to', async () => {
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', topic: ['orders'] },
      positionals: ['my-group'],
    });
    await expect(groupResetOffsetsCommand.run(context)).rejects.toThrow(/--to/);
  });

  it('rejects an unrecognized --to value naming the two valid choices', async () => {
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', topic: ['orders'], to: 'bogus' },
      positionals: ['my-group'],
    });
    await expect(groupResetOffsetsCommand.run(context)).rejects.toThrow(/earliest, latest/);
  });

  it('dry-run previews the new offsets without calling resetOffsets or confirming', async () => {
    const resetOffsets = vi.fn(async () => {});
    const fetchTopicOffsets = vi.fn(async () => [{ partition: 0, offset: 5n, high: 10n, low: 2n }]);
    const admin = createFakeAdmin({ fetchTopicOffsets, resetOffsets, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', topic: ['orders'], to: 'earliest' },
      positionals: ['my-group'],
      openAdmin: async () => admin,
    });

    const code = await groupResetOffsetsCommand.run(context);

    expect(code).toBe(0);
    expect(fetchTopicOffsets).toHaveBeenCalledWith('orders');
    expect(resetOffsets).not.toHaveBeenCalled();
  });

  it('dry-run json reports the earliest/latest-resolved new offset per partition', async () => {
    const fetchTopicOffsets = vi.fn(async () => [{ partition: 0, offset: 5n, high: 10n, low: 2n }]);
    const admin = createFakeAdmin({ fetchTopicOffsets, disconnect: async () => {} });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', topic: ['orders'], to: 'latest' },
      positionals: ['my-group'],
      openAdmin: async () => admin,
      format: 'json',
    });

    await groupResetOffsetsCommand.run(context);
    const written = JSON.parse(stdoutWrite.mock.calls[0]![0]) as {
      topics: { topic: string; partitions: { partition: number; newOffset: string }[] }[];
    };
    expect(written.topics[0]!.partitions[0]).toEqual({ partition: 0, newOffset: '10' });
  });

  it('--execute aborts without --yes off a TTY', async () => {
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', topic: ['orders'], to: 'earliest', execute: true },
      positionals: ['my-group'],
    });
    await expect(groupResetOffsetsCommand.run(context)).rejects.toThrow(CliAbortedError);
  });

  it('--execute with --yes resets each --topic with the resolved earliest/latest flag', async () => {
    const resetOffsets = vi.fn(async () => {});
    const admin = createFakeAdmin({ resetOffsets, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', topic: ['orders'], to: 'earliest', execute: true, yes: true },
      positionals: ['my-group'],
      openAdmin: async () => admin,
    });

    const code = await groupResetOffsetsCommand.run(context);

    expect(code).toBe(0);
    expect(resetOffsets).toHaveBeenCalledWith({ groupId: 'my-group', topic: 'orders', earliest: true });
  });

  it('fans out one resetOffsets call per --topic when more than one is given', async () => {
    const resetOffsets = vi.fn(async () => {});
    const admin = createFakeAdmin({ resetOffsets, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', topic: ['orders', 'payments'], to: 'latest', execute: true, yes: true },
      positionals: ['my-group'],
      openAdmin: async () => admin,
    });

    const code = await groupResetOffsetsCommand.run(context);

    expect(code).toBe(0);
    expect(resetOffsets).toHaveBeenCalledTimes(2);
    expect(resetOffsets).toHaveBeenCalledWith({ groupId: 'my-group', topic: 'orders', earliest: false });
    expect(resetOffsets).toHaveBeenCalledWith({ groupId: 'my-group', topic: 'payments', earliest: false });
  });

  it('returns exit 4 on a fanned-out --execute partial failure', async () => {
    const resetOffsets = vi.fn(async ({ topic }: { topic: string }) => {
      if (topic === 'payments') throw new Error('boom');
    });
    const admin = createFakeAdmin({ resetOffsets, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', topic: ['orders', 'payments'], to: 'earliest', execute: true, yes: true },
      positionals: ['my-group'],
      openAdmin: async () => admin,
    });

    const code = await groupResetOffsetsCommand.run(context);
    expect(code).toBe(4);
  });

  it('returns exit 1 when every fanned-out --execute call fails', async () => {
    const resetOffsets = vi.fn(async () => {
      throw new Error('boom');
    });
    const admin = createFakeAdmin({ resetOffsets, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', topic: ['orders', 'payments'], to: 'earliest', execute: true, yes: true },
      positionals: ['my-group'],
      openAdmin: async () => admin,
    });

    const code = await groupResetOffsetsCommand.run(context);
    expect(code).toBe(1);
  });

  it('disconnects even when the dry-run preview call throws', async () => {
    const disconnect = vi.fn(async () => {});
    const admin = createFakeAdmin({
      fetchTopicOffsets: async () => {
        throw new Error('boom');
      },
      disconnect,
    });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', topic: ['orders'], to: 'earliest' },
      positionals: ['my-group'],
      openAdmin: async () => admin,
    });

    await expect(groupResetOffsetsCommand.run(context)).rejects.toThrow('boom');
    expect(disconnect).toHaveBeenCalledTimes(1);
  });
});
