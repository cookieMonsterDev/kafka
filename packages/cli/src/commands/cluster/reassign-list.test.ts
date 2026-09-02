import { describe, expect, it, vi } from 'vitest';
import { createFakeAdmin } from '../../testing/create-fake-admin';
import { createFakeCommandContext } from '../../testing/create-command-context';
import { clusterReassignListCommand } from './reassign-list';

function fakeReassignments() {
  return {
    topics: [
      {
        name: 'orders',
        partitions: [{ partition: 0, replicas: [1, 2, 3], addingReplicas: [3], removingReplicas: [1] }],
      },
    ],
  };
}

describe('clusterReassignListCommand', () => {
  it('renders one row per partition', async () => {
    const listPartitionReassignments = async () => fakeReassignments();
    const admin = createFakeAdmin({ listPartitionReassignments, disconnect: async () => {} });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      openAdmin: async () => admin,
    });

    const code = await clusterReassignListCommand.run(context);

    expect(code).toBe(0);
    const written = stdoutWrite.mock.calls[0]![0];
    expect(written).toContain('orders');
    expect(written).toContain('1, 2, 3');
  });

  it('renders "(none)" for empty adding/removing replicas', async () => {
    const listPartitionReassignments = async () => ({
      topics: [
        { name: 'orders', partitions: [{ partition: 0, replicas: [1], addingReplicas: [], removingReplicas: [] }] },
      ],
    });
    const admin = createFakeAdmin({ listPartitionReassignments, disconnect: async () => {} });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      openAdmin: async () => admin,
    });

    await clusterReassignListCommand.run(context);

    expect(stdoutWrite.mock.calls[0]![0]).toContain('(none)');
  });

  it('renders a placeholder when there are no active reassignments', async () => {
    const listPartitionReassignments = async () => ({ topics: [] });
    const admin = createFakeAdmin({ listPartitionReassignments, disconnect: async () => {} });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      openAdmin: async () => admin,
    });

    await clusterReassignListCommand.run(context);

    expect(stdoutWrite.mock.calls[0]![0]).toContain('(no active reassignments)');
  });

  it('passes --timeout through, and takes no topic filter', async () => {
    const listPartitionReassignments = vi.fn(async () => fakeReassignments());
    const admin = createFakeAdmin({ listPartitionReassignments, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', timeout: 5000 },
      openAdmin: async () => admin,
    });

    await clusterReassignListCommand.run(context);

    expect(listPartitionReassignments).toHaveBeenCalledWith({ timeout: 5000 });
  });

  it('reports json output with the raw topics shape', async () => {
    const listPartitionReassignments = async () => fakeReassignments();
    const admin = createFakeAdmin({ listPartitionReassignments, disconnect: async () => {} });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      openAdmin: async () => admin,
      format: 'json',
    });

    await clusterReassignListCommand.run(context);

    const written = JSON.parse(stdoutWrite.mock.calls[0]![0]) as ReturnType<typeof fakeReassignments>;
    expect(written).toEqual(fakeReassignments());
  });

  it('disconnects even when listPartitionReassignments throws', async () => {
    const disconnect = vi.fn(async () => {});
    const admin = createFakeAdmin({
      listPartitionReassignments: async () => {
        throw new Error('boom');
      },
      disconnect,
    });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      openAdmin: async () => admin,
    });

    await expect(clusterReassignListCommand.run(context)).rejects.toThrow('boom');
    expect(disconnect).toHaveBeenCalledTimes(1);
  });
});
