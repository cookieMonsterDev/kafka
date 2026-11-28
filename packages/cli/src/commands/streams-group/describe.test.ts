import type { StreamsGroupDescription } from '@cookiemonsterdev/kafka-core';
import { describe, expect, it, vi } from 'vitest';
import { createFakeAdmin } from '../../testing/create-fake-admin';
import { createFakeCommandContext } from '../../testing/create-command-context';
import { streamsGroupDescribeCommand } from './describe';

function fakeGroup(groupId: string, overrides: Partial<StreamsGroupDescription> = {}): StreamsGroupDescription {
  const member = {
    memberId: 'm1',
    memberEpoch: 3,
    instanceId: null,
    rackId: null,
    clientId: 'c1',
    clientHost: '/127.0.0.1',
    topologyEpoch: 1,
    processId: 'p1',
    userEndpoint: null,
    clientTags: [],
    taskOffsets: [],
    taskEndOffsets: [],
    assignment: { activeTasks: [], standbyTasks: [], warmupTasks: [] },
    targetAssignment: { activeTasks: [], standbyTasks: [], warmupTasks: [] },
    isClassic: false,
  };
  return {
    errorCode: 0,
    errorMessage: null,
    groupId,
    groupState: 'Stable',
    groupEpoch: 3,
    assignmentEpoch: 3,
    topology: { epoch: 1, subtopologies: [] },
    members: [member, { ...member, memberId: 'm2' }],
    authorizedOperations: -2147483648,
    topologyDescription: null,
    topologyDescriptionStatus: 0,
    assignorName: 'sticky',
    ...overrides,
  };
}

function groupIdNotFound(message: string): Error {
  // Admin wraps the broker's non-retriable error, so its type is on `cause`.
  return Object.assign(new Error(message), {
    name: 'KafkaNonRetriableError',
    cause: { name: 'KafkaProtocolError', type: 'GROUP_ID_NOT_FOUND' },
  });
}

describe('streamsGroupDescribeCommand', () => {
  it('requires at least one group id', async () => {
    const { context } = createFakeCommandContext({ flags: { brokers: 'localhost:9092' }, positionals: [] });
    await expect(streamsGroupDescribeCommand.run(context)).rejects.toThrow(/at least one group/);
  });

  it('renders state, epochs, assignor and member count from one call', async () => {
    const describeStreamsGroups = vi.fn(async () => ({ groups: [fakeGroup('word-count')] }));
    const admin = createFakeAdmin({ describeStreamsGroups, disconnect: async () => {} });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      positionals: ['word-count'],
      openAdmin: async () => admin,
    });

    const code = await streamsGroupDescribeCommand.run(context);

    expect(code).toBe(0);
    expect(describeStreamsGroups).toHaveBeenCalledWith(['word-count'], { includeTopologyDescription: false });
    const written = stdoutWrite.mock.calls[0]![0];
    expect(written).toContain('TOPOLOGY EPOCH');
    expect(written).toContain('Stable');
    expect(written).toContain('sticky');
    expect(written).not.toContain('TOPOLOGY DESCRIPTION');
  });

  it('asks for and reports the topology description with --topology-description', async () => {
    const topologyDescription = { subtopologies: [{ subtopologyId: '0', nodes: [] }], globalStores: [] };
    const describeStreamsGroups = vi.fn(async () => ({
      groups: [fakeGroup('word-count', { topologyDescription, topologyDescriptionStatus: 3 })],
    }));
    const admin = createFakeAdmin({ describeStreamsGroups, disconnect: async () => {} });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', 'topology-description': true },
      positionals: ['word-count'],
      openAdmin: async () => admin,
      format: 'json',
    });

    await streamsGroupDescribeCommand.run(context);

    expect(describeStreamsGroups).toHaveBeenCalledWith(['word-count'], { includeTopologyDescription: true });
    const written = JSON.parse(stdoutWrite.mock.calls[0]![0]) as { groups: unknown[] };
    expect(written.groups).toEqual([
      {
        groupId: 'word-count',
        ok: true,
        state: 'Stable',
        epoch: 3,
        topologyEpoch: 1,
        assignor: 'sticky',
        memberCount: 2,
        topologyDescriptionStatus: 'AVAILABLE',
        topologyDescription,
      },
    ]);
  });

  it('reports GROUP_ID_NOT_FOUND as a failure with the broker message', async () => {
    const admin = createFakeAdmin({
      describeStreamsGroups: async () => {
        throw groupIdNotFound('Group orders is not a streams group.');
      },
      disconnect: async () => {},
    });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      positionals: ['orders'],
      openAdmin: async () => admin,
      format: 'json',
    });

    expect(await streamsGroupDescribeCommand.run(context)).toBe(1);
    const written = JSON.parse(stdoutWrite.mock.calls[0]![0]) as { groups: unknown[] };
    expect(written.groups).toEqual([{ groupId: 'orders', ok: false, detail: 'Group orders is not a streams group.' }]);
  });

  it('fans out one call per group and exits 4 on a partial failure', async () => {
    const describeStreamsGroups = vi.fn(async (groupIds: string[]) => {
      if (groupIds[0] === 'missing') throw groupIdNotFound('Group missing not found.');
      return { groups: [fakeGroup(groupIds[0]!)] };
    });
    const admin = createFakeAdmin({ describeStreamsGroups, disconnect: async () => {} });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      positionals: ['word-count', 'missing'],
      openAdmin: async () => admin,
    });

    expect(await streamsGroupDescribeCommand.run(context)).toBe(4);
    expect(describeStreamsGroups).toHaveBeenCalledTimes(2);
    expect(stdoutWrite.mock.calls[0]![0]).toContain('Group missing not found.');
  });

  it('returns exit 1 when every fanned-out call fails', async () => {
    const admin = createFakeAdmin({
      describeStreamsGroups: async () => {
        throw new Error('boom');
      },
      disconnect: async () => {},
    });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      positionals: ['a', 'b'],
      openAdmin: async () => admin,
    });

    expect(await streamsGroupDescribeCommand.run(context)).toBe(1);
  });

  it('treats a missing result as a failure for that group', async () => {
    const admin = createFakeAdmin({ describeStreamsGroups: async () => ({ groups: [] }), disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      positionals: ['word-count'],
      openAdmin: async () => admin,
    });

    expect(await streamsGroupDescribeCommand.run(context)).toBe(1);
  });

  it('disconnects even when a single-group call throws', async () => {
    const disconnect = vi.fn(async () => {});
    const admin = createFakeAdmin({
      describeStreamsGroups: async () => {
        throw new Error('boom');
      },
      disconnect,
    });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      positionals: ['word-count'],
      openAdmin: async () => admin,
    });

    await expect(streamsGroupDescribeCommand.run(context)).rejects.toThrow('boom');
    expect(disconnect).toHaveBeenCalledTimes(1);
  });
});
