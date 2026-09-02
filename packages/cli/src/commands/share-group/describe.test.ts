import { describe, expect, it, vi } from 'vitest';
import { createFakeAdmin } from '../../testing/create-fake-admin';
import { createFakeCommandContext } from '../../testing/create-command-context';
import { shareGroupDescribeCommand } from './describe';

function fakeGroup(groupId: string) {
  return {
    errorCode: 0,
    errorMessage: null,
    groupId,
    groupState: 'Stable',
    groupEpoch: 3,
    assignmentEpoch: 3,
    assignorName: 'simple',
    members: [
      {
        memberId: 'm1',
        rackId: null,
        memberEpoch: 1,
        clientId: 'c1',
        clientHost: 'h1',
        subscribedTopicNames: ['orders'],
        assignment: { topicPartitions: [] },
      },
      {
        memberId: 'm2',
        rackId: null,
        memberEpoch: 1,
        clientId: 'c2',
        clientHost: 'h2',
        subscribedTopicNames: ['orders'],
        assignment: { topicPartitions: [] },
      },
    ],
    authorizedOperations: 0,
  };
}

describe('shareGroupDescribeCommand', () => {
  it('requires at least one group id', async () => {
    const { context } = createFakeCommandContext({ flags: { brokers: 'localhost:9092' }, positionals: [] });
    await expect(shareGroupDescribeCommand.run(context)).rejects.toThrow(/at least one group/);
  });

  it('describes a single group with one call', async () => {
    const describeShareGroups = vi.fn(async () => ({ groups: [fakeGroup('g1')] }));
    const admin = createFakeAdmin({ describeShareGroups, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      positionals: ['g1'],
      openAdmin: async () => admin,
    });

    const code = await shareGroupDescribeCommand.run(context);

    expect(code).toBe(0);
    expect(describeShareGroups).toHaveBeenCalledWith(['g1']);
  });

  it('fans out one call per group when more than one group id is given', async () => {
    const describeShareGroups = vi.fn(async (groupIds: string[]) => ({ groups: [fakeGroup(groupIds[0]!)] }));
    const admin = createFakeAdmin({ describeShareGroups, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      positionals: ['g1', 'g2'],
      openAdmin: async () => admin,
    });

    const code = await shareGroupDescribeCommand.run(context);

    expect(code).toBe(0);
    expect(describeShareGroups).toHaveBeenCalledTimes(2);
    expect(describeShareGroups).toHaveBeenCalledWith(['g1']);
    expect(describeShareGroups).toHaveBeenCalledWith(['g2']);
  });

  it('returns exit 4 on a fanned-out partial failure', async () => {
    const describeShareGroups = vi.fn(async (groupIds: string[]) => {
      if (groupIds[0] === 'g2') throw new Error('GROUP_ID_NOT_FOUND');
      return { groups: [fakeGroup(groupIds[0]!)] };
    });
    const admin = createFakeAdmin({ describeShareGroups, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      positionals: ['g1', 'g2'],
      openAdmin: async () => admin,
    });

    expect(await shareGroupDescribeCommand.run(context)).toBe(4);
  });

  it('returns exit 1 when every fanned-out call fails', async () => {
    const describeShareGroups = vi.fn(async () => {
      throw new Error('boom');
    });
    const admin = createFakeAdmin({ describeShareGroups, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      positionals: ['g1', 'g2'],
      openAdmin: async () => admin,
    });

    expect(await shareGroupDescribeCommand.run(context)).toBe(1);
  });

  it('renders state, epoch, assignor, and member count', async () => {
    const admin = createFakeAdmin({
      describeShareGroups: async () => ({ groups: [fakeGroup('g1')] }),
      disconnect: async () => {},
    });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      positionals: ['g1'],
      openAdmin: async () => admin,
    });

    await shareGroupDescribeCommand.run(context);
    const written = stdoutWrite.mock.calls[0]![0];
    expect(written).toContain('Stable');
    expect(written).toContain('simple');
    expect(written).toContain('2');
  });

  it('treats a missing result as a failure for that group', async () => {
    const admin = createFakeAdmin({ describeShareGroups: async () => ({ groups: [] }), disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      positionals: ['g1'],
      openAdmin: async () => admin,
    });

    expect(await shareGroupDescribeCommand.run(context)).toBe(1);
  });

  it('disconnects even when a single-group call throws', async () => {
    const disconnect = vi.fn(async () => {});
    const admin = createFakeAdmin({
      describeShareGroups: async () => {
        throw new Error('boom');
      },
      disconnect,
    });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      positionals: ['g1'],
      openAdmin: async () => admin,
    });

    await expect(shareGroupDescribeCommand.run(context)).rejects.toThrow('boom');
    expect(disconnect).toHaveBeenCalledTimes(1);
  });
});
