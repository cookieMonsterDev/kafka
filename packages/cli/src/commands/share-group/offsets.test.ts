import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import type { Admin } from '@cookiemonsterdev/kafka-core';
import { afterEach, describe, expect, it, vi } from 'vitest';
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

  it('fans out one call per topic when more than one --topic filter is given', async () => {
    const listShareGroupOffsets = vi.fn(async (options: Parameters<Admin['listShareGroupOffsets']>[0]) => ({
      groups: [
        {
          groupId: 'g1',
          errorCode: 0,
          errorMessage: null,
          topics: [{ topicName: options.groups[0]!.topics![0]!.topicName, topicId: Buffer.alloc(16), partitions: [] }],
        },
      ],
    }));
    const admin = createFakeAdmin({ listShareGroupOffsets, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', topic: ['orders', 'payments'] },
      positionals: ['g1'],
      openAdmin: async () => admin,
    });

    const code = await shareGroupOffsetsCommand.run(context);

    expect(code).toBe(0);
    expect(listShareGroupOffsets).toHaveBeenCalledTimes(2);
    expect(listShareGroupOffsets).toHaveBeenCalledWith({
      groups: [{ groupId: 'g1', topics: [{ topicName: 'orders' }] }],
    });
    expect(listShareGroupOffsets).toHaveBeenCalledWith({
      groups: [{ groupId: 'g1', topics: [{ topicName: 'payments' }] }],
    });
  });

  it('returns exit 4 on a fanned-out partial read failure', async () => {
    const listShareGroupOffsets = vi.fn(async (options: Parameters<Admin['listShareGroupOffsets']>[0]) => {
      const topicName = options.groups[0]!.topics![0]!.topicName;
      if (topicName === 'payments') throw new Error('UNKNOWN_TOPIC_OR_PARTITION');
      return {
        groups: [
          {
            groupId: 'g1',
            errorCode: 0,
            errorMessage: null,
            topics: [{ topicName, topicId: Buffer.alloc(16), partitions: [] }],
          },
        ],
      };
    });
    const admin = createFakeAdmin({ listShareGroupOffsets, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', topic: ['orders', 'payments'] },
      positionals: ['g1'],
      openAdmin: async () => admin,
    });

    expect(await shareGroupOffsetsCommand.run(context)).toBe(4);
  });

  it('disconnects even when a fanned-out read call throws', async () => {
    const disconnect = vi.fn(async () => {});
    const admin = createFakeAdmin({
      listShareGroupOffsets: async () => {
        throw new Error('boom');
      },
      disconnect,
    });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', topic: ['orders', 'payments'] },
      positionals: ['g1'],
      openAdmin: async () => admin,
    });

    await shareGroupOffsetsCommand.run(context);
    expect(disconnect).toHaveBeenCalledTimes(1);
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

describe('shareGroupOffsetsCommand — --from-file mode', () => {
  let tmpDir: string | undefined;

  afterEach(() => {
    if (tmpDir !== undefined) {
      rmSync(tmpDir, { recursive: true, force: true });
      tmpDir = undefined;
    }
  });

  function writeOffsetsFile(contents: string): string {
    tmpDir = mkdtempSync(join(tmpdir(), 'kafka-cli-share-group-offsets-'));
    const file = join(tmpDir, 'offsets.csv');
    writeFileSync(file, contents);
    return file;
  }

  it('sets the offsets the file lists, one call per topic', async () => {
    const file = writeOffsetsFile('orders,0,1000\norders,1,2000\npayments,0,0\n');
    const alterShareGroupOffsets = vi.fn(async () => ({ responses: [] }));
    const admin = createFakeAdmin({ alterShareGroupOffsets, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', 'from-file': file, yes: true },
      positionals: ['g1'],
      openAdmin: async () => admin,
    });

    expect(await shareGroupOffsetsCommand.run(context)).toBe(0);
    expect(alterShareGroupOffsets).toHaveBeenCalledTimes(2);
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
    expect(alterShareGroupOffsets).toHaveBeenCalledWith({
      groupId: 'g1',
      topics: [{ topicName: 'payments', partitions: [{ partitionIndex: 0, startOffset: 0n }] }],
    });
  });

  it('uses only the share group’s records from a group,topic,partition,offset file', async () => {
    const file = writeOffsetsFile('other,orders,0,5\ng1,orders,0,1000\n');
    const alterShareGroupOffsets = vi.fn(async () => ({ responses: [] }));
    const admin = createFakeAdmin({ alterShareGroupOffsets, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', 'from-file': file, yes: true },
      positionals: ['g1'],
      openAdmin: async () => admin,
    });

    await shareGroupOffsetsCommand.run(context);
    expect(alterShareGroupOffsets).toHaveBeenCalledWith({
      groupId: 'g1',
      topics: [{ topicName: 'orders', partitions: [{ partitionIndex: 0, startOffset: 1000n }] }],
    });
  });

  it('reports an unreadable file as a usage error naming the flag', async () => {
    const openAdmin = vi.fn();
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', 'from-file': '/nonexistent/offsets.csv', yes: true },
      positionals: ['g1'],
      openAdmin,
    });

    const run = shareGroupOffsetsCommand.run(context);
    await expect(run).rejects.toThrow(CliUsageError);
    await expect(run).rejects.toThrow(/could not read --from-file "\/nonexistent\/offsets.csv"/);
    expect(openAdmin).not.toHaveBeenCalled();
  });

  it('rejects an invalid record before prompting or connecting', async () => {
    const file = writeOffsetsFile('orders,0,1000\norders,x,2000\n');
    const openAdmin = vi.fn();
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', 'from-file': file },
      positionals: ['g1'],
      openAdmin,
    });

    await expect(shareGroupOffsetsCommand.run(context)).rejects.toThrow(
      /line 2: partition must be an integer from 0 to 2147483647/,
    );
    expect(openAdmin).not.toHaveBeenCalled();
  });

  it('aborts without --yes off a TTY', async () => {
    const file = writeOffsetsFile('orders,0,1000\n');
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', 'from-file': file },
      positionals: ['g1'],
    });
    await expect(shareGroupOffsetsCommand.run(context)).rejects.toThrow(CliAbortedError);
  });

  it.each([[{ set: ['orders:0:1000'] }], [{ 'delete-topic': ['orders'] }]])(
    'rejects combining --from-file with %j',
    async (otherFlags) => {
      const { context } = createFakeCommandContext({
        flags: { brokers: 'localhost:9092', 'from-file': 'offsets.csv', yes: true, ...otherFlags },
        positionals: ['g1'],
      });
      await expect(shareGroupOffsetsCommand.run(context)).rejects.toThrow(
        'share-group offsets accepts at most one of --set, --from-file, or --delete-topic',
      );
    },
  );
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
