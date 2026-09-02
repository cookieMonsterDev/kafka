import { describe, expect, it, vi } from 'vitest';
import { createFakeAdmin } from '../../testing/create-fake-admin';
import { createFakeCommandContext } from '../../testing/create-command-context';
import { shareGroupDescribeCommand } from './describe';

describe('shareGroupDescribeCommand', () => {
  it('requires at least one group id', async () => {
    const { context } = createFakeCommandContext({ flags: { brokers: 'localhost:9092' }, positionals: [] });
    await expect(shareGroupDescribeCommand.run(context)).rejects.toThrow(/at least one group/);
  });

  it('describes every positional in a single call', async () => {
    const describeShareGroups = vi.fn(async () => ({ groups: [] }));
    const admin = createFakeAdmin({ describeShareGroups, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      positionals: ['g1', 'g2'],
      openAdmin: async () => admin,
    });

    const code = await shareGroupDescribeCommand.run(context);

    expect(code).toBe(0);
    expect(describeShareGroups).toHaveBeenCalledWith(['g1', 'g2']);
  });

  it('renders state, epoch, assignor, and member count', async () => {
    const admin = createFakeAdmin({
      describeShareGroups: async () => ({
        groups: [
          {
            errorCode: 0,
            errorMessage: null,
            groupId: 'g1',
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

    await shareGroupDescribeCommand.run(context);
    const written = stdoutWrite.mock.calls[0]![0];
    expect(written).toContain('Stable');
    expect(written).toContain('simple');
    expect(written).toContain('2');
  });

  it('surfaces a per-group error in the STATE column', async () => {
    const admin = createFakeAdmin({
      describeShareGroups: async () => ({
        groups: [
          {
            errorCode: 69,
            errorMessage: 'group id not found',
            groupId: 'ghost',
            groupState: '',
            groupEpoch: 0,
            assignmentEpoch: 0,
            assignorName: '',
            members: [],
            authorizedOperations: 0,
          },
        ],
      }),
      disconnect: async () => {},
    });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      positionals: ['ghost'],
      openAdmin: async () => admin,
    });

    await shareGroupDescribeCommand.run(context);
    expect(stdoutWrite.mock.calls[0]![0]).toContain('error (code 69)');
  });

  it('disconnects even when describeShareGroups throws', async () => {
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
