import { describe, expect, it, vi } from 'vitest';
import { createFakeAdmin } from '../../testing/create-fake-admin';
import { createFakeCommandContext } from '../../testing/create-command-context';
import { pingCommand } from './ping';

describe('pingCommand', () => {
  it('describes the cluster and reports ok', async () => {
    const disconnect = vi.fn(async () => {});
    const admin = createFakeAdmin({
      describeCluster: async () => ({
        brokers: [{ nodeId: 1, host: 'localhost', port: 9092 }],
        controller: 1,
        clusterId: 'test-cluster',
      }),
      disconnect,
    });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      openAdmin: async () => admin,
    });

    const code = await pingCommand.run(context);

    expect(code).toBe(0);
    expect(disconnect).toHaveBeenCalledTimes(1);
    expect(stdoutWrite).toHaveBeenCalledWith(expect.stringContaining('test-cluster'));
  });

  it('disconnects even when describeCluster throws', async () => {
    const disconnect = vi.fn(async () => {});
    const admin = createFakeAdmin({
      describeCluster: async () => {
        throw new Error('connection refused');
      },
      disconnect,
    });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      openAdmin: async () => admin,
    });

    await expect(pingCommand.run(context)).rejects.toThrow('connection refused');
    expect(disconnect).toHaveBeenCalledTimes(1);
  });

  it('throws a usage error when --brokers is missing', async () => {
    const { context } = createFakeCommandContext({});
    await expect(pingCommand.run(context)).rejects.toThrow(/--brokers/);
  });
});
