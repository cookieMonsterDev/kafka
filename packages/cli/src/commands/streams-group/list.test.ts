import { describe, expect, it, vi } from 'vitest';
import { createFakeAdmin } from '../../testing/create-fake-admin';
import { createFakeCommandContext } from '../../testing/create-command-context';
import { streamsGroupListCommand } from './list';

describe('streamsGroupListCommand', () => {
  it('filters listGroups to protocolType "streams"', async () => {
    const listGroups = vi.fn(async () => ({
      groups: [
        { groupId: 'consumer-group-1', protocolType: 'consumer' },
        { groupId: 'word-count', protocolType: 'streams', groupState: 'Stable', groupType: 'streams' },
      ],
    }));
    const admin = createFakeAdmin({ listGroups, disconnect: async () => {} });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      openAdmin: async () => admin,
      format: 'json',
    });

    const code = await streamsGroupListCommand.run(context);

    expect(code).toBe(0);
    expect(listGroups).toHaveBeenCalledWith();
    const written = JSON.parse(stdoutWrite.mock.calls[0]![0]) as { groups: unknown[] };
    expect(written.groups).toEqual([
      { groupId: 'word-count', protocolType: 'streams', groupState: 'Stable', groupType: 'streams' },
    ]);
  });

  it('renders "(no streams groups)" when nothing matches', async () => {
    const admin = createFakeAdmin({ listGroups: async () => ({ groups: [] }), disconnect: async () => {} });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      openAdmin: async () => admin,
    });

    await streamsGroupListCommand.run(context);
    expect(stdoutWrite.mock.calls[0]![0]).toContain('(no streams groups)');
  });

  it('renders a table row per streams group with its state', async () => {
    const admin = createFakeAdmin({
      listGroups: async () => ({
        groups: [{ groupId: 'word-count', protocolType: 'streams', groupState: 'NotReady' }],
      }),
      disconnect: async () => {},
    });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      openAdmin: async () => admin,
    });

    await streamsGroupListCommand.run(context);
    const written = stdoutWrite.mock.calls[0]![0];
    expect(written).toContain('word-count');
    expect(written).toContain('NotReady');
  });

  it('disconnects even when listGroups throws', async () => {
    const disconnect = vi.fn(async () => {});
    const admin = createFakeAdmin({
      listGroups: async () => {
        throw new Error('boom');
      },
      disconnect,
    });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      openAdmin: async () => admin,
    });

    await expect(streamsGroupListCommand.run(context)).rejects.toThrow('boom');
    expect(disconnect).toHaveBeenCalledTimes(1);
  });
});
