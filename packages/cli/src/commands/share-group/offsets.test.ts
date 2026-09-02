import { describe, expect, it, vi } from 'vitest';
import { CliAbortedError } from '../../errors/aborted-error';
import { CliUsageError } from '../../args/coerce';
import { createFakeAdmin } from '../../testing/create-fake-admin';
import { createFakeCommandContext, EMPTY_RESOLVED_CLI_CONFIG } from '../../testing/create-command-context';
import { shareGroupOffsetsCommand } from './offsets';

describe('shareGroupOffsetsCommand — read mode', () => {
  it('requires a group id', async () => {
    const { context } = createFakeCommandContext({ flags: { brokers: 'localhost:9092' }, positionals: [] });
    await expect(shareGroupOffsetsCommand.run(context)).rejects.toThrow(/requires a group id/);
  });

  it('reads with no topic filter by default', async () => {
    const listShareGroupOffsets = vi.fn(async () => ({ groups: [] }));
    const admin = createFakeAdmin({ listShareGroupOffsets, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      positionals: ['g1'],
      openAdmin: async () => admin,
    });

    const code = await shareGroupOffsetsCommand.run(context);

    expect(code).toBe(0);
    expect(listShareGroupOffsets).toHaveBeenCalledWith({ groups: [{ groupId: 'g1', topics: undefined }] });
  });

  it('passes --topic through as a topic filter', async () => {
    const listShareGroupOffsets = vi.fn(async () => ({ groups: [] }));
    const admin = createFakeAdmin({ listShareGroupOffsets, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', topic: ['orders'] },
      positionals: ['g1'],
      openAdmin: async () => admin,
    });

    await shareGroupOffsetsCommand.run(context);
    expect(listShareGroupOffsets).toHaveBeenCalledWith({
      groups: [{ groupId: 'g1', topics: [{ topicName: 'orders' }] }],
    });
  });

  it('renders start offset and lag per partition', async () => {
    const admin = createFakeAdmin({
      listShareGroupOffsets: async () => ({
        groups: [
          {
            groupId: 'g1',
            errorCode: 0,
            errorMessage: null,
            topics: [
              {
                topicName: 'orders',
                topicId: Buffer.alloc(16),
                partitions: [
                  { partitionIndex: 0, startOffset: 100n, leaderEpoch: 1, lag: 5n, errorCode: 0, errorMessage: null },
                ],
              },
            ],
          },
        ],
      }),
      disconnect: async () => {},
    });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      positionals: ['g1'],
      openAdmin: async () => admin,
    });

    await shareGroupOffsetsCommand.run(context);
    const written = stdoutWrite.mock.calls[0]![0];
    expect(written).toContain('orders');
    expect(written).toContain('100');
    expect(written).toContain('5');
  });
});

describe('shareGroupOffsetsCommand — --set mode', () => {
  it('rejects a malformed --set entry', async () => {
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', set: ['orders:0'], yes: true },
      positionals: ['g1'],
    });
    await expect(shareGroupOffsetsCommand.run(context)).rejects.toThrow(CliUsageError);
  });

  it('aborts without --yes off a TTY', async () => {
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', set: ['orders:0:1000'] },
      positionals: ['g1'],
    });
    await expect(shareGroupOffsetsCommand.run(context)).rejects.toThrow(CliAbortedError);
  });

  it('skips confirmation when cli.confirmDestructive is false', async () => {
    const alterShareGroupOffsets = vi.fn(async () => ({ responses: [] }));
    const admin = createFakeAdmin({ alterShareGroupOffsets, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', set: ['orders:0:1000'] },
      positionals: ['g1'],
      openAdmin: async () => admin,
      config: { ...EMPTY_RESOLVED_CLI_CONFIG, cli: { confirmDestructive: false } },
    });

    expect(await shareGroupOffsetsCommand.run(context)).toBe(0);
  });

  it('groups multiple partitions for the same topic into one call', async () => {
    const alterShareGroupOffsets = vi.fn(async () => ({ responses: [] }));
    const admin = createFakeAdmin({ alterShareGroupOffsets, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', set: ['orders:0:1000', 'orders:1:2000'], yes: true },
      positionals: ['g1'],
      openAdmin: async () => admin,
    });

    await shareGroupOffsetsCommand.run(context);

    expect(alterShareGroupOffsets).toHaveBeenCalledTimes(1);
    expect(alterShareGroupOffsets).toHaveBeenCalledWith({
      groupId: 'g1',
      topics: [
        {
          topicName: 'orders',
          partitions: [
            { partitionIndex: 0, startOffset: 1000n },
            { partitionIndex: 1, startOffset: 2000n },
          ],
        },
      ],
    });
  });

  it('fans out one call per topic when --set spans more than one topic', async () => {
    const alterShareGroupOffsets = vi.fn(async () => ({ responses: [] }));
    const admin = createFakeAdmin({ alterShareGroupOffsets, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', set: ['orders:0:1000', 'payments:0:0'], yes: true },
      positionals: ['g1'],
      openAdmin: async () => admin,
    });

    const code = await shareGroupOffsetsCommand.run(context);

    expect(code).toBe(0);
    expect(alterShareGroupOffsets).toHaveBeenCalledTimes(2);
    expect(alterShareGroupOffsets).toHaveBeenCalledWith({
      groupId: 'g1',
      topics: [{ topicName: 'orders', partitions: [{ partitionIndex: 0, startOffset: 1000n }] }],
    });
    expect(alterShareGroupOffsets).toHaveBeenCalledWith({
      groupId: 'g1',
      topics: [{ topicName: 'payments', partitions: [{ partitionIndex: 0, startOffset: 0n }] }],
    });
  });

  it('returns exit 4 on a fanned-out partial failure', async () => {
    const alterShareGroupOffsets = vi.fn(async (options: { topics: { topicName: string }[] }) => {
      if (options.topics[0]!.topicName === 'payments') throw new Error('UNKNOWN_TOPIC_OR_PARTITION');
      return { responses: [] };
    });
    const admin = createFakeAdmin({ alterShareGroupOffsets, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', set: ['orders:0:1000', 'payments:0:0'], yes: true },
      positionals: ['g1'],
      openAdmin: async () => admin,
    });

    expect(await shareGroupOffsetsCommand.run(context)).toBe(4);
  });

  it('propagates a single-topic failure rather than reporting a result row', async () => {
    const admin = createFakeAdmin({
      alterShareGroupOffsets: async () => {
        throw new Error('UNKNOWN_TOPIC_OR_PARTITION');
      },
      disconnect: async () => {},
    });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', set: ['orders:0:1000'], yes: true },
      positionals: ['g1'],
      openAdmin: async () => admin,
    });

    await expect(shareGroupOffsetsCommand.run(context)).rejects.toThrow('UNKNOWN_TOPIC_OR_PARTITION');
  });

  it('disconnects even when a single-topic call throws', async () => {
    const disconnect = vi.fn(async () => {});
    const admin = createFakeAdmin({
      alterShareGroupOffsets: async () => {
        throw new Error('boom');
      },
      disconnect,
    });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', set: ['orders:0:1000'], yes: true },
      positionals: ['g1'],
      openAdmin: async () => admin,
    });

    await expect(shareGroupOffsetsCommand.run(context)).rejects.toThrow('boom');
    expect(disconnect).toHaveBeenCalledTimes(1);
  });
});

describe('shareGroupOffsetsCommand — --delete-topic mode', () => {
  it('rejects combining --set and --delete-topic', async () => {
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', set: ['orders:0:1000'], 'delete-topic': ['orders'], yes: true },
      positionals: ['g1'],
    });
    await expect(shareGroupOffsetsCommand.run(context)).rejects.toThrow(CliUsageError);
  });

  it('aborts without --yes off a TTY', async () => {
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', 'delete-topic': ['orders'] },
      positionals: ['g1'],
    });
    await expect(shareGroupOffsetsCommand.run(context)).rejects.toThrow(CliAbortedError);
  });

  it('deletes offsets for a single topic with one call', async () => {
    const deleteShareGroupOffsets = vi.fn(async () => ({ responses: [] }));
    const admin = createFakeAdmin({ deleteShareGroupOffsets, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', 'delete-topic': ['orders'], yes: true },
      positionals: ['g1'],
      openAdmin: async () => admin,
    });

    const code = await shareGroupOffsetsCommand.run(context);

    expect(code).toBe(0);
    expect(deleteShareGroupOffsets).toHaveBeenCalledTimes(1);
    expect(deleteShareGroupOffsets).toHaveBeenCalledWith({ groupId: 'g1', topics: ['orders'] });
  });

  it('fans out one call per topic when more than one --delete-topic is given', async () => {
    const deleteShareGroupOffsets = vi.fn(async () => ({ responses: [] }));
    const admin = createFakeAdmin({ deleteShareGroupOffsets, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', 'delete-topic': ['orders', 'payments'], yes: true },
      positionals: ['g1'],
      openAdmin: async () => admin,
    });

    const code = await shareGroupOffsetsCommand.run(context);

    expect(code).toBe(0);
    expect(deleteShareGroupOffsets).toHaveBeenCalledTimes(2);
    expect(deleteShareGroupOffsets).toHaveBeenCalledWith({ groupId: 'g1', topics: ['orders'] });
    expect(deleteShareGroupOffsets).toHaveBeenCalledWith({ groupId: 'g1', topics: ['payments'] });
  });

  it('returns exit 4 on a fanned-out partial failure', async () => {
    const deleteShareGroupOffsets = vi.fn(async (options: { topics: string[] }) => {
      if (options.topics[0] === 'payments') throw new Error('UNKNOWN_TOPIC_OR_PARTITION');
      return { responses: [] };
    });
    const admin = createFakeAdmin({ deleteShareGroupOffsets, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', 'delete-topic': ['orders', 'payments'], yes: true },
      positionals: ['g1'],
      openAdmin: async () => admin,
    });

    expect(await shareGroupOffsetsCommand.run(context)).toBe(4);
  });

  it('disconnects even when a single-topic deleteShareGroupOffsets call throws', async () => {
    const disconnect = vi.fn(async () => {});
    const admin = createFakeAdmin({
      deleteShareGroupOffsets: async () => {
        throw new Error('boom');
      },
      disconnect,
    });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', 'delete-topic': ['orders'], yes: true },
      positionals: ['g1'],
      openAdmin: async () => admin,
    });

    await expect(shareGroupOffsetsCommand.run(context)).rejects.toThrow('boom');
    expect(disconnect).toHaveBeenCalledTimes(1);
  });
});
