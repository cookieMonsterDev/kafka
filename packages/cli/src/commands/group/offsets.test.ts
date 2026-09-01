import { describe, expect, it, vi } from 'vitest';
import { createFakeAdmin } from '../../testing/create-fake-admin';
import { createFakeCommandContext } from '../../testing/create-command-context';
import { groupOffsetsCommand } from './offsets';

describe('groupOffsetsCommand', () => {
  it('requires a group id', async () => {
    const { context } = createFakeCommandContext({ flags: { brokers: 'localhost:9092' }, positionals: [] });
    await expect(groupOffsetsCommand.run(context)).rejects.toThrow(/requires a group id/);
  });

  it('fetches offsets for a group with resolveOffsets always false', async () => {
    const fetchOffsets = vi.fn(async () => [
      { topic: 'orders', partitions: [{ partition: 0, offset: 42n, metadata: null }] },
    ]);
    const admin = createFakeAdmin({ fetchOffsets, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      positionals: ['my-group'],
      openAdmin: async () => admin,
    });

    const code = await groupOffsetsCommand.run(context);

    expect(code).toBe(0);
    expect(fetchOffsets).toHaveBeenCalledWith({ groupId: 'my-group', topics: undefined, resolveOffsets: false });
  });

  it('passes --topic through as the topics filter', async () => {
    const fetchOffsets = vi.fn(async () => []);
    const admin = createFakeAdmin({ fetchOffsets, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', topic: ['orders', 'payments'] },
      positionals: ['my-group'],
      openAdmin: async () => admin,
    });

    await groupOffsetsCommand.run(context);
    expect(fetchOffsets).toHaveBeenCalledWith({
      groupId: 'my-group',
      topics: ['orders', 'payments'],
      resolveOffsets: false,
    });
  });

  it('renders "(no offsets)" when the group has none', async () => {
    const fetchOffsets = vi.fn(async () => []);
    const admin = createFakeAdmin({ fetchOffsets, disconnect: async () => {} });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      positionals: ['my-group'],
      openAdmin: async () => admin,
    });

    await groupOffsetsCommand.run(context);
    expect(stdoutWrite.mock.calls[0]![0]).toContain('(no offsets)');
  });

  it('json output carries the bigint offset as a string', async () => {
    const fetchOffsets = vi.fn(async () => [
      { topic: 'orders', partitions: [{ partition: 0, offset: 9007199254740993n, metadata: null }] },
    ]);
    const admin = createFakeAdmin({ fetchOffsets, disconnect: async () => {} });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      positionals: ['my-group'],
      openAdmin: async () => admin,
      format: 'json',
    });

    await groupOffsetsCommand.run(context);
    const written = JSON.parse(stdoutWrite.mock.calls[0]![0]) as {
      topic: string;
      partitions: { offset: string }[];
    }[];
    expect(written[0]!.partitions[0]!.offset).toBe('9007199254740993');
  });

  it('disconnects even when fetchOffsets throws', async () => {
    const disconnect = vi.fn(async () => {});
    const admin = createFakeAdmin({
      fetchOffsets: async () => {
        throw new Error('boom');
      },
      disconnect,
    });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      positionals: ['my-group'],
      openAdmin: async () => admin,
    });

    await expect(groupOffsetsCommand.run(context)).rejects.toThrow('boom');
    expect(disconnect).toHaveBeenCalledTimes(1);
  });
});
