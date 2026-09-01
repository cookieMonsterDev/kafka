import { describe, expect, it, vi } from 'vitest';
import { CliUsageError } from '../../args/coerce';
import { createFakeAdmin } from '../../testing/create-fake-admin';
import { createFakeCommandContext } from '../../testing/create-command-context';
import { configUnsetCommand } from './unset';

describe('configUnsetCommand', () => {
  it('removes a single key from a single resource', async () => {
    const incrementalAlterConfigs = vi.fn(async () => ({ resources: [] }));
    const admin = createFakeAdmin({ incrementalAlterConfigs, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', type: 'topic', key: ['retention.ms'] },
      positionals: ['orders'],
      openAdmin: async () => admin,
    });

    const code = await configUnsetCommand.run(context);

    expect(code).toBe(0);
    expect(incrementalAlterConfigs).toHaveBeenCalledWith({
      resources: [
        {
          type: 2,
          name: 'orders',
          configs: [{ name: 'retention.ms', value: null, operation: 1 }],
        },
      ],
      validateOnly: false,
    });
  });

  it('passes multiple --key flags as multiple delete operations', async () => {
    const incrementalAlterConfigs = vi.fn(async (_options: { resources: unknown[] }) => ({ resources: [] }));
    const admin = createFakeAdmin({ incrementalAlterConfigs, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', type: 'topic', key: ['retention.ms', 'cleanup.policy'] },
      positionals: ['orders'],
      openAdmin: async () => admin,
    });

    await configUnsetCommand.run(context);
    const call = incrementalAlterConfigs.mock.calls[0]![0] as { resources: { configs: unknown[] }[] };
    expect(call.resources[0]!.configs).toEqual([
      { name: 'retention.ms', value: null, operation: 1 },
      { name: 'cleanup.policy', value: null, operation: 1 },
    ]);
  });

  it('maps --dry-run to validateOnly', async () => {
    const incrementalAlterConfigs = vi.fn(async () => ({ resources: [] }));
    const admin = createFakeAdmin({ incrementalAlterConfigs, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', type: 'topic', key: ['retention.ms'], 'dry-run': true },
      positionals: ['orders'],
      openAdmin: async () => admin,
    });

    await configUnsetCommand.run(context);
    expect(incrementalAlterConfigs).toHaveBeenCalledWith(expect.objectContaining({ validateOnly: true }));
  });

  it('requires at least one resource name', async () => {
    const { context } = createFakeCommandContext({
      flags: { type: 'topic', key: ['a'] },
      positionals: [],
    });
    await expect(configUnsetCommand.run(context)).rejects.toThrow(/at least one resource name/);
  });

  it('requires --type', async () => {
    const { context } = createFakeCommandContext({
      flags: { key: ['a'] },
      positionals: ['orders'],
    });
    await expect(configUnsetCommand.run(context)).rejects.toThrow(/--type/);
  });

  it('rejects an unknown --type', async () => {
    const { context } = createFakeCommandContext({
      flags: { type: 'bogus', key: ['a'] },
      positionals: ['orders'],
    });
    await expect(configUnsetCommand.run(context)).rejects.toThrow(CliUsageError);
  });

  it('requires at least one --key', async () => {
    const { context } = createFakeCommandContext({
      flags: { type: 'topic' },
      positionals: ['orders'],
    });
    await expect(configUnsetCommand.run(context)).rejects.toThrow(/--key/);
  });

  it('fans out one call per resource when more than one name is given', async () => {
    const incrementalAlterConfigs = vi.fn(async () => ({ resources: [] }));
    const admin = createFakeAdmin({ incrementalAlterConfigs, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', type: 'topic', key: ['retention.ms'] },
      positionals: ['orders', 'payments'],
      openAdmin: async () => admin,
    });

    const code = await configUnsetCommand.run(context);

    expect(code).toBe(0);
    expect(incrementalAlterConfigs).toHaveBeenCalledTimes(2);
    expect(incrementalAlterConfigs).toHaveBeenCalledWith(
      expect.objectContaining({ resources: [expect.objectContaining({ name: 'orders' })] }),
    );
    expect(incrementalAlterConfigs).toHaveBeenCalledWith(
      expect.objectContaining({ resources: [expect.objectContaining({ name: 'payments' })] }),
    );
  });

  it('returns exit 4 on a fanned-out partial failure', async () => {
    const incrementalAlterConfigs = vi.fn(async ({ resources }: { resources: { name: string }[] }) => {
      if (resources[0]!.name === 'payments') throw new Error('boom');
      return { resources: [] };
    });
    const admin = createFakeAdmin({ incrementalAlterConfigs, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', type: 'topic', key: ['retention.ms'] },
      positionals: ['orders', 'payments'],
      openAdmin: async () => admin,
    });

    const code = await configUnsetCommand.run(context);
    expect(code).toBe(4);
  });

  it('returns exit 1 when every fanned-out call fails', async () => {
    const incrementalAlterConfigs = vi.fn(async () => {
      throw new Error('boom');
    });
    const admin = createFakeAdmin({ incrementalAlterConfigs, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', type: 'topic', key: ['retention.ms'] },
      positionals: ['orders', 'payments'],
      openAdmin: async () => admin,
    });

    const code = await configUnsetCommand.run(context);
    expect(code).toBe(1);
  });

  it('disconnects even when a single-resource call throws', async () => {
    const disconnect = vi.fn(async () => {});
    const admin = createFakeAdmin({
      incrementalAlterConfigs: async () => {
        throw new Error('boom');
      },
      disconnect,
    });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', type: 'topic', key: ['retention.ms'] },
      positionals: ['orders'],
      openAdmin: async () => admin,
    });

    await expect(configUnsetCommand.run(context)).rejects.toThrow('boom');
    expect(disconnect).toHaveBeenCalledTimes(1);
  });
});
