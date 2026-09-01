import { describe, expect, it, vi } from 'vitest';
import { createFakeAdmin } from '../../testing/create-fake-admin';
import { createFakeCommandContext } from '../../testing/create-command-context';
import { topicOffsetsCommand } from './offsets';

describe('topicOffsetsCommand', () => {
  it('requires a topic name', async () => {
    const { context } = createFakeCommandContext({ flags: { brokers: 'localhost:9092' }, positionals: [] });
    await expect(topicOffsetsCommand.run(context)).rejects.toThrow(/requires a topic name/);
  });

  it('fetches plain high/low offsets when --time is omitted', async () => {
    const fetchTopicOffsets = vi.fn(async () => [{ partition: 0, offset: 10n, high: 10n, low: 0n }]);
    const admin = createFakeAdmin({ fetchTopicOffsets, disconnect: async () => {} });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      positionals: ['orders'],
      openAdmin: async () => admin,
      format: 'json',
    });

    const code = await topicOffsetsCommand.run(context);

    expect(code).toBe(0);
    expect(fetchTopicOffsets).toHaveBeenCalledWith('orders');
    const written = JSON.parse(stdoutWrite.mock.calls[0]?.[0] ?? '{}') as { partitions: { offset: string }[] };
    expect(written.partitions[0]?.offset).toBe('10');
  });

  it('renders a human table with PARTITION/OFFSET/HIGH/LOW when --time is omitted', async () => {
    const fetchTopicOffsets = vi.fn(async () => [{ partition: 0, offset: 10n, high: 10n, low: 0n }]);
    const admin = createFakeAdmin({ fetchTopicOffsets, disconnect: async () => {} });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      positionals: ['orders'],
      openAdmin: async () => admin,
    });

    await topicOffsetsCommand.run(context);
    const written = stdoutWrite.mock.calls[0]?.[0] ?? '';
    expect(written).toContain('PARTITION');
    expect(written).toContain('HIGH');
    expect(written).toContain('LOW');
  });

  it('resolves --time earliest to the -2 sentinel', async () => {
    const fetchTopicOffsetsByTimestamp = vi.fn(async () => [{ partition: 0, offset: 0n }]);
    const admin = createFakeAdmin({ fetchTopicOffsetsByTimestamp, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', time: 'earliest' },
      positionals: ['orders'],
      openAdmin: async () => admin,
    });

    await topicOffsetsCommand.run(context);
    expect(fetchTopicOffsetsByTimestamp).toHaveBeenCalledWith('orders', -2n);
  });

  it('resolves --time latest to the -1 sentinel', async () => {
    const fetchTopicOffsetsByTimestamp = vi.fn(async () => [{ partition: 0, offset: 10n }]);
    const admin = createFakeAdmin({ fetchTopicOffsetsByTimestamp, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', time: 'latest' },
      positionals: ['orders'],
      openAdmin: async () => admin,
    });

    await topicOffsetsCommand.run(context);
    expect(fetchTopicOffsetsByTimestamp).toHaveBeenCalledWith('orders', -1n);
  });

  it('resolves --time max-timestamp to the -3 sentinel', async () => {
    const fetchTopicOffsetsByTimestamp = vi.fn(async () => [{ partition: 0, offset: 10n }]);
    const admin = createFakeAdmin({ fetchTopicOffsetsByTimestamp, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', time: 'max-timestamp' },
      positionals: ['orders'],
      openAdmin: async () => admin,
    });

    await topicOffsetsCommand.run(context);
    expect(fetchTopicOffsetsByTimestamp).toHaveBeenCalledWith('orders', -3n);
  });

  it('resolves a numeric --time to a literal millisecond timestamp', async () => {
    const fetchTopicOffsetsByTimestamp = vi.fn(async () => [{ partition: 0, offset: 5n }]);
    const admin = createFakeAdmin({ fetchTopicOffsetsByTimestamp, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', time: '1735689600000' },
      positionals: ['orders'],
      openAdmin: async () => admin,
    });

    await topicOffsetsCommand.run(context);
    expect(fetchTopicOffsetsByTimestamp).toHaveBeenCalledWith('orders', 1735689600000n);
  });

  it('rejects an invalid --time value', async () => {
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', time: 'yesterday' },
      positionals: ['orders'],
    });
    await expect(topicOffsetsCommand.run(context)).rejects.toThrow(/--time must be/);
  });

  it('renders a bigint offset as a JSON string via --time', async () => {
    const fetchTopicOffsetsByTimestamp = vi.fn(async () => [{ partition: 0, offset: 9007199254740993n }]);
    const admin = createFakeAdmin({ fetchTopicOffsetsByTimestamp, disconnect: async () => {} });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', time: 'latest' },
      positionals: ['orders'],
      openAdmin: async () => admin,
      format: 'json',
    });

    await topicOffsetsCommand.run(context);
    const written = stdoutWrite.mock.calls[0]?.[0] ?? '';
    expect(written).toContain('"9007199254740993"');
  });

  it('disconnects even when the fetch throws', async () => {
    const disconnect = vi.fn(async () => {});
    const admin = createFakeAdmin({
      fetchTopicOffsets: async () => {
        throw new Error('boom');
      },
      disconnect,
    });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      positionals: ['orders'],
      openAdmin: async () => admin,
    });

    await expect(topicOffsetsCommand.run(context)).rejects.toThrow('boom');
    expect(disconnect).toHaveBeenCalledTimes(1);
  });
});
