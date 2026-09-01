import { describe, expect, it, vi } from 'vitest';
import { createFakeAdmin } from '../../testing/create-fake-admin';
import { createFakeCommandContext } from '../../testing/create-command-context';
import { groupListCommand } from './list';

describe('groupListCommand', () => {
  it('lists every group', async () => {
    const listGroups = vi.fn(async () => ({
      groups: [
        { groupId: 'g1', protocolType: 'consumer' },
        { groupId: 'g2', protocolType: 'consumer' },
      ],
    }));
    const admin = createFakeAdmin({ listGroups, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      openAdmin: async () => admin,
    });

    const code = await groupListCommand.run(context);

    expect(code).toBe(0);
    expect(listGroups).toHaveBeenCalledWith();
  });

  it('renders "(no groups)" when the cluster has none', async () => {
    const listGroups = vi.fn(async () => ({ groups: [] }));
    const admin = createFakeAdmin({ listGroups, disconnect: async () => {} });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      openAdmin: async () => admin,
    });

    const code = await groupListCommand.run(context);

    expect(code).toBe(0);
    expect(stdoutWrite.mock.calls[0]![0]).toContain('(no groups)');
  });

  it('reports json output with the raw group list', async () => {
    const listGroups = vi.fn(async () => ({ groups: [{ groupId: 'g1', protocolType: 'consumer' }] }));
    const admin = createFakeAdmin({ listGroups, disconnect: async () => {} });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      openAdmin: async () => admin,
      format: 'json',
    });

    await groupListCommand.run(context);
    const written = JSON.parse(stdoutWrite.mock.calls[0]![0]) as { groups: { groupId: string }[] };
    expect(written.groups).toEqual([{ groupId: 'g1', protocolType: 'consumer' }]);
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

    await expect(groupListCommand.run(context)).rejects.toThrow('boom');
    expect(disconnect).toHaveBeenCalledTimes(1);
  });
});
