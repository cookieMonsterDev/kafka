import { describe, expect, it, vi } from 'vitest';
import { createFakeAdmin } from '../../testing/create-fake-admin';
import { createFakeCommandContext } from '../../testing/create-command-context';
import { shareGroupListCommand } from './list';

describe('shareGroupListCommand', () => {
  it('filters listGroups to protocolType "share"', async () => {
    const listGroups = vi.fn(async () => ({
      groups: [
        { groupId: 'consumer-group-1', protocolType: 'consumer' },
        { groupId: 'share-group-1', protocolType: 'share' },
      ],
    }));
    const admin = createFakeAdmin({ listGroups, disconnect: async () => {} });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      openAdmin: async () => admin,
      format: 'json',
    });

    const code = await shareGroupListCommand.run(context);

    expect(code).toBe(0);
    const written = JSON.parse(stdoutWrite.mock.calls[0]![0]) as { groups: { groupId: string }[] };
    expect(written.groups).toEqual([{ groupId: 'share-group-1', protocolType: 'share' }]);
  });

  it('renders "(no share groups)" when nothing matches', async () => {
    const admin = createFakeAdmin({ listGroups: async () => ({ groups: [] }), disconnect: async () => {} });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      openAdmin: async () => admin,
    });

    await shareGroupListCommand.run(context);
    expect(stdoutWrite.mock.calls[0]![0]).toContain('(no share groups)');
  });

  it('renders a table row per share group', async () => {
    const admin = createFakeAdmin({
      listGroups: async () => ({ groups: [{ groupId: 'share-group-1', protocolType: 'share' }] }),
      disconnect: async () => {},
    });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      openAdmin: async () => admin,
    });

    await shareGroupListCommand.run(context);
    expect(stdoutWrite.mock.calls[0]![0]).toContain('share-group-1');
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

    await expect(shareGroupListCommand.run(context)).rejects.toThrow('boom');
    expect(disconnect).toHaveBeenCalledTimes(1);
  });
});
