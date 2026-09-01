import { describe, expect, it, vi } from 'vitest';
import { createFakeAdmin } from '../../testing/create-fake-admin';
import { createFakeCommandContext } from '../../testing/create-command-context';
import { groupDescribeCommand } from './describe';

function fakeGroup(groupId: string) {
  return {
    errorCode: 0,
    groupId,
    state: 'Stable',
    protocolType: 'consumer',
    protocol: 'range',
    members: [
      {
        memberId: 'member-1',
        clientId: 'client-1',
        clientHost: '/127.0.0.1',
        memberMetadata: Buffer.from('meta'),
        memberAssignment: Buffer.from('assign'),
      },
    ],
  };
}

describe('groupDescribeCommand', () => {
  it('describes a single group', async () => {
    const describeGroups = vi.fn(async () => ({ groups: [fakeGroup('g1')] }));
    const admin = createFakeAdmin({ describeGroups, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      positionals: ['g1'],
      openAdmin: async () => admin,
    });

    const code = await groupDescribeCommand.run(context);

    expect(code).toBe(0);
    expect(describeGroups).toHaveBeenCalledWith(['g1']);
  });

  it('requires at least one group id', async () => {
    const { context } = createFakeCommandContext({ flags: { brokers: 'localhost:9092' }, positionals: [] });
    await expect(groupDescribeCommand.run(context)).rejects.toThrow(/at least one group/);
  });

  it('fans out one call per group when more than one group id is given', async () => {
    const describeGroups = vi.fn(async (groupIds: string[]) => ({ groups: [fakeGroup(groupIds[0]!)] }));
    const admin = createFakeAdmin({ describeGroups, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      positionals: ['g1', 'g2'],
      openAdmin: async () => admin,
    });

    const code = await groupDescribeCommand.run(context);

    expect(code).toBe(0);
    expect(describeGroups).toHaveBeenCalledTimes(2);
    expect(describeGroups).toHaveBeenCalledWith(['g1']);
    expect(describeGroups).toHaveBeenCalledWith(['g2']);
  });

  it('returns exit 4 on a fanned-out partial failure', async () => {
    const describeGroups = vi.fn(async (groupIds: string[]) => {
      if (groupIds[0] === 'g2') throw new Error('boom');
      return { groups: [fakeGroup(groupIds[0]!)] };
    });
    const admin = createFakeAdmin({ describeGroups, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      positionals: ['g1', 'g2'],
      openAdmin: async () => admin,
    });

    const code = await groupDescribeCommand.run(context);
    expect(code).toBe(4);
  });

  it('returns exit 1 when every fanned-out call fails', async () => {
    const describeGroups = vi.fn(async () => {
      throw new Error('boom');
    });
    const admin = createFakeAdmin({ describeGroups, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      positionals: ['g1', 'g2'],
      openAdmin: async () => admin,
    });

    const code = await groupDescribeCommand.run(context);
    expect(code).toBe(1);
  });

  it('json output includes member identity fields but omits the raw metadata/assignment buffers', async () => {
    const describeGroups = vi.fn(async () => ({ groups: [fakeGroup('g1')] }));
    const admin = createFakeAdmin({ describeGroups, disconnect: async () => {} });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      positionals: ['g1'],
      openAdmin: async () => admin,
      format: 'json',
    });

    await groupDescribeCommand.run(context);
    const written = JSON.parse(stdoutWrite.mock.calls[0]![0]) as {
      groups: {
        groupId: string;
        ok: boolean;
        state: string;
        members: Record<string, unknown>[];
      }[];
    };
    expect(written.groups[0]!.groupId).toBe('g1');
    expect(written.groups[0]!.ok).toBe(true);
    expect(written.groups[0]!.state).toBe('Stable');
    const member = written.groups[0]!.members[0]!;
    expect(member).toEqual({ memberId: 'member-1', clientId: 'client-1', clientHost: '/127.0.0.1' });
    expect(member.memberMetadata).toBeUndefined();
    expect(member.memberAssignment).toBeUndefined();
  });

  it('treats a missing result as a failure for that group', async () => {
    const describeGroups = vi.fn(async () => ({ groups: [] }));
    const admin = createFakeAdmin({ describeGroups, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      positionals: ['g1'],
      openAdmin: async () => admin,
    });

    const code = await groupDescribeCommand.run(context);
    expect(code).toBe(1);
  });

  it('treats a "Dead" group state as a failure, not a successful describe', async () => {
    // The broker reports a group id it has never seen (or has fully forgotten) with
    // `errorCode: 0` and `state: "Dead"` rather than an error — this must not read as success.
    const describeGroups = vi.fn(async () => ({ groups: [{ ...fakeGroup('g1'), state: 'Dead', members: [] }] }));
    const admin = createFakeAdmin({ describeGroups, disconnect: async () => {} });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      positionals: ['g1'],
      openAdmin: async () => admin,
      format: 'json',
    });

    const code = await groupDescribeCommand.run(context);

    expect(code).toBe(1);
    const written = JSON.parse(stdoutWrite.mock.calls[0]![0]) as {
      groups: { groupId: string; ok: boolean; detail?: string }[];
    };
    expect(written.groups[0]).toEqual({ groupId: 'g1', ok: false, detail: 'group does not exist' });
  });

  it('disconnects even when a single-group call throws', async () => {
    const disconnect = vi.fn(async () => {});
    const admin = createFakeAdmin({
      describeGroups: async () => {
        throw new Error('boom');
      },
      disconnect,
    });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      positionals: ['g1'],
      openAdmin: async () => admin,
    });

    await expect(groupDescribeCommand.run(context)).rejects.toThrow('boom');
    expect(disconnect).toHaveBeenCalledTimes(1);
  });
});
