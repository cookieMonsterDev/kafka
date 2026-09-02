import { describe, expect, it, vi } from 'vitest';
import { createFakeAdmin } from '../../testing/create-fake-admin';
import { createFakeCommandContext } from '../../testing/create-command-context';
import { clusterInfoCommand } from './info';

function fakeClusterDescription() {
  return {
    brokers: [
      { nodeId: 1, host: 'broker-1', port: 9092 },
      { nodeId: 2, host: 'broker-2', port: 9092 },
    ],
    controller: 1,
    clusterId: 'test-cluster',
  };
}

describe('clusterInfoCommand', () => {
  it('describes the cluster and renders a broker table', async () => {
    const describeCluster = async () => fakeClusterDescription();
    const admin = createFakeAdmin({ describeCluster, disconnect: async () => {} });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      openAdmin: async () => admin,
    });

    const code = await clusterInfoCommand.run(context);

    expect(code).toBe(0);
    const written = stdoutWrite.mock.calls[0]![0];
    expect(written).toContain('Cluster ID: test-cluster');
    expect(written).toContain('Controller: 1');
    expect(written).toContain('broker-1');
    expect(written).toContain('broker-2');
  });

  it('renders "(none)" for a missing controller and cluster id', async () => {
    const describeCluster = async () => ({ brokers: [], controller: null, clusterId: null });
    const admin = createFakeAdmin({ describeCluster, disconnect: async () => {} });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      openAdmin: async () => admin,
    });

    await clusterInfoCommand.run(context);

    const written = stdoutWrite.mock.calls[0]![0];
    expect(written).toContain('Cluster ID: (none)');
    expect(written).toContain('Controller: (none)');
  });

  it('reports json output with the raw result shape', async () => {
    const describeCluster = async () => fakeClusterDescription();
    const admin = createFakeAdmin({ describeCluster, disconnect: async () => {} });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      openAdmin: async () => admin,
      format: 'json',
    });

    await clusterInfoCommand.run(context);

    const written = JSON.parse(stdoutWrite.mock.calls[0]![0]) as ReturnType<typeof fakeClusterDescription>;
    expect(written).toEqual(fakeClusterDescription());
  });

  it('disconnects even when describeCluster throws', async () => {
    const disconnect = vi.fn(async () => {});
    const admin = createFakeAdmin({
      describeCluster: async () => {
        throw new Error('boom');
      },
      disconnect,
    });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      openAdmin: async () => admin,
    });

    await expect(clusterInfoCommand.run(context)).rejects.toThrow('boom');
    expect(disconnect).toHaveBeenCalledTimes(1);
  });
});
