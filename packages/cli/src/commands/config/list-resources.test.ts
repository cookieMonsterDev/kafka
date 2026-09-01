import { describe, expect, it, vi } from 'vitest';
import { createFakeAdmin } from '../../testing/create-fake-admin';
import { createFakeCommandContext } from '../../testing/create-command-context';
import { configListResourcesCommand } from './list-resources';

describe('configListResourcesCommand', () => {
  it('lists every resource with no --type filter', async () => {
    const listConfigResources = vi.fn(async () => ({
      resources: [
        { resourceName: 'orders', resourceType: 2 },
        { resourceName: '1', resourceType: 4 },
      ],
    }));
    const admin = createFakeAdmin({ listConfigResources, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      openAdmin: async () => admin,
    });

    const code = await configListResourcesCommand.run(context);

    expect(code).toBe(0);
    expect(listConfigResources).toHaveBeenCalledWith({ resourceTypes: undefined });
  });

  it('resolves --type into numeric resource type codes', async () => {
    const listConfigResources = vi.fn(async () => ({ resources: [] }));
    const admin = createFakeAdmin({ listConfigResources, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', type: ['topic', 'group'] },
      openAdmin: async () => admin,
    });

    await configListResourcesCommand.run(context);
    expect(listConfigResources).toHaveBeenCalledWith({ resourceTypes: [2, 32] });
  });

  it('reports json output with the raw resource list', async () => {
    const listConfigResources = vi.fn(async () => ({
      resources: [{ resourceName: 'orders', resourceType: 2 }],
    }));
    const admin = createFakeAdmin({ listConfigResources, disconnect: async () => {} });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      openAdmin: async () => admin,
      format: 'json',
    });

    await configListResourcesCommand.run(context);
    const written = JSON.parse(stdoutWrite.mock.calls[0]![0]) as {
      resources: { resourceName: string; resourceType: number }[];
    };
    expect(written.resources).toEqual([{ resourceName: 'orders', resourceType: 2 }]);
  });

  it('disconnects even when the call throws', async () => {
    const disconnect = vi.fn(async () => {});
    const admin = createFakeAdmin({
      listConfigResources: async () => {
        throw new Error('boom');
      },
      disconnect,
    });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      openAdmin: async () => admin,
    });

    await expect(configListResourcesCommand.run(context)).rejects.toThrow('boom');
    expect(disconnect).toHaveBeenCalledTimes(1);
  });
});
