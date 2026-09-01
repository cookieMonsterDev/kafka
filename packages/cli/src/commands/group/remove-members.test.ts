import { describe, expect, it, vi } from 'vitest';
import { CliAbortedError } from '../../errors/aborted-error';
import { createFakeAdmin } from '../../testing/create-fake-admin';
import { createFakeCommandContext } from '../../testing/create-command-context';
import { groupRemoveMembersCommand } from './remove-members';

describe('groupRemoveMembersCommand', () => {
  it('requires a group id', async () => {
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', yes: true, member: ['m1'] },
      positionals: [],
    });
    await expect(groupRemoveMembersCommand.run(context)).rejects.toThrow(/requires a group id/);
  });

  it('requires at least one --member', async () => {
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', yes: true },
      positionals: ['my-group'],
    });
    await expect(groupRemoveMembersCommand.run(context)).rejects.toThrow(/--member/);
  });

  it('aborts without --yes off a TTY', async () => {
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', member: ['m1'] },
      positionals: ['my-group'],
    });
    await expect(groupRemoveMembersCommand.run(context)).rejects.toThrow(CliAbortedError);
  });

  it('splits a bare memberId and a memberId:groupInstanceId pair on the first colon', async () => {
    const removeMembersFromConsumerGroup = vi.fn(async () => ({
      members: [
        { memberId: 'm1', groupInstanceId: null, errorCode: 0 },
        { memberId: 'm2', groupInstanceId: 'inst-1', errorCode: 0 },
      ],
    }));
    const admin = createFakeAdmin({ removeMembersFromConsumerGroup, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', yes: true, member: ['m1', 'm2:inst-1'] },
      positionals: ['my-group'],
      openAdmin: async () => admin,
    });

    const code = await groupRemoveMembersCommand.run(context);

    expect(code).toBe(0);
    expect(removeMembersFromConsumerGroup).toHaveBeenCalledWith({
      groupId: 'my-group',
      members: [{ memberId: 'm1' }, { memberId: 'm2', groupInstanceId: 'inst-1' }],
    });
  });

  it('returns exit 4 when some members fail and others succeed', async () => {
    const removeMembersFromConsumerGroup = vi.fn(async () => ({
      members: [
        { memberId: 'm1', groupInstanceId: null, errorCode: 0 },
        { memberId: 'm2', groupInstanceId: null, errorCode: 25 },
      ],
    }));
    const admin = createFakeAdmin({ removeMembersFromConsumerGroup, disconnect: async () => {} });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', yes: true, member: ['m1', 'm2'] },
      positionals: ['my-group'],
      openAdmin: async () => admin,
      format: 'json',
    });

    const code = await groupRemoveMembersCommand.run(context);

    expect(code).toBe(4);
    const written = JSON.parse(stdoutWrite.mock.calls[0]![0]) as {
      members: { memberId: string; errorCode: number; ok: boolean }[];
    };
    expect(written.members).toEqual([
      { memberId: 'm1', groupInstanceId: null, errorCode: 0, ok: true },
      { memberId: 'm2', groupInstanceId: null, errorCode: 25, ok: false },
    ]);
  });

  it('returns exit 1 when every member fails', async () => {
    const removeMembersFromConsumerGroup = vi.fn(async () => ({
      members: [{ memberId: 'm1', groupInstanceId: null, errorCode: 25 }],
    }));
    const admin = createFakeAdmin({ removeMembersFromConsumerGroup, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', yes: true, member: ['m1'] },
      positionals: ['my-group'],
      openAdmin: async () => admin,
    });

    const code = await groupRemoveMembersCommand.run(context);
    expect(code).toBe(1);
  });

  it('renders a failed member as "failed (code N)" in human output', async () => {
    const removeMembersFromConsumerGroup = vi.fn(async () => ({
      members: [{ memberId: 'm1', groupInstanceId: null, errorCode: 25 }],
    }));
    const admin = createFakeAdmin({ removeMembersFromConsumerGroup, disconnect: async () => {} });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', yes: true, member: ['m1'] },
      positionals: ['my-group'],
      openAdmin: async () => admin,
    });

    await groupRemoveMembersCommand.run(context);
    expect(stdoutWrite.mock.calls[0]![0]).toContain('failed (code 25)');
  });

  it('disconnects even when removeMembersFromConsumerGroup throws', async () => {
    const disconnect = vi.fn(async () => {});
    const admin = createFakeAdmin({
      removeMembersFromConsumerGroup: async () => {
        throw new Error('boom');
      },
      disconnect,
    });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', yes: true, member: ['m1'] },
      positionals: ['my-group'],
      openAdmin: async () => admin,
    });

    await expect(groupRemoveMembersCommand.run(context)).rejects.toThrow('boom');
    expect(disconnect).toHaveBeenCalledTimes(1);
  });
});
