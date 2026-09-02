import { describe, expect, it, vi } from 'vitest';
import { createFakeAdmin } from '../../testing/create-fake-admin';
import { createFakeCommandContext } from '../../testing/create-command-context';
import { quotaDescribeCommand } from './describe';

describe('quotaDescribeCommand', () => {
  it('describes with no components/strict by default', async () => {
    const describeClientQuotas = vi.fn(async () => ({ entries: [] }));
    const admin = createFakeAdmin({ describeClientQuotas, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      openAdmin: async () => admin,
    });

    const code = await quotaDescribeCommand.run(context);

    expect(code).toBe(0);
    expect(describeClientQuotas).toHaveBeenCalledWith({ components: [], strict: false });
  });

  it('builds components from --entity and --entity-any, and passes --strict', async () => {
    const describeClientQuotas = vi.fn(async () => ({ entries: [] }));
    const admin = createFakeAdmin({ describeClientQuotas, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', entity: { user: 'alice' }, 'entity-any': ['client-id'], strict: true },
      openAdmin: async () => admin,
    });

    await quotaDescribeCommand.run(context);

    expect(describeClientQuotas).toHaveBeenCalledWith({
      components: [
        { entityType: 'user', matchType: 0, match: 'alice' },
        { entityType: 'client-id', matchType: 2, match: null },
      ],
      strict: true,
    });
  });

  it('renders "(no matching quotas)" when nothing matches', async () => {
    const admin = createFakeAdmin({ describeClientQuotas: async () => ({ entries: [] }), disconnect: async () => {} });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      openAdmin: async () => admin,
    });

    await quotaDescribeCommand.run(context);
    expect(stdoutWrite.mock.calls[0]![0]).toContain('(no matching quotas)');
  });

  it('renders one row per entity/value pair, formatting the entity', async () => {
    const admin = createFakeAdmin({
      describeClientQuotas: async () => ({
        entries: [
          {
            entity: [{ entityType: 'user', entityName: 'alice' }],
            values: [{ key: 'producer_byte_rate', value: 1048576 }],
          },
        ],
      }),
      disconnect: async () => {},
    });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      openAdmin: async () => admin,
    });

    await quotaDescribeCommand.run(context);
    const written = stdoutWrite.mock.calls[0]![0];
    expect(written).toContain('user=alice');
    expect(written).toContain('producer_byte_rate');
    expect(written).toContain('1048576');
  });

  it('formats the cluster-default entity with no components', async () => {
    const admin = createFakeAdmin({
      describeClientQuotas: async () => ({
        entries: [{ entity: [], values: [{ key: 'producer_byte_rate', value: 1 }] }],
      }),
      disconnect: async () => {},
    });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      openAdmin: async () => admin,
    });

    await quotaDescribeCommand.run(context);
    expect(stdoutWrite.mock.calls[0]![0]).toContain('(cluster default)');
  });

  it('reports json output with the raw entries shape', async () => {
    const entries = [{ entity: [{ entityType: 'user', entityName: 'alice' }], values: [] }];
    const admin = createFakeAdmin({ describeClientQuotas: async () => ({ entries }), disconnect: async () => {} });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      openAdmin: async () => admin,
      format: 'json',
    });

    await quotaDescribeCommand.run(context);
    const written = JSON.parse(stdoutWrite.mock.calls[0]![0]) as { entries: unknown };
    expect(written.entries).toEqual(entries);
  });

  it('disconnects even when describeClientQuotas throws', async () => {
    const disconnect = vi.fn(async () => {});
    const admin = createFakeAdmin({
      describeClientQuotas: async () => {
        throw new Error('boom');
      },
      disconnect,
    });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      openAdmin: async () => admin,
    });

    await expect(quotaDescribeCommand.run(context)).rejects.toThrow('boom');
    expect(disconnect).toHaveBeenCalledTimes(1);
  });
});
