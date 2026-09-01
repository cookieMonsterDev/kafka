import { describe, expect, it, vi } from 'vitest';
import { CliUsageError } from '../../args/coerce';
import { createFakeAdmin } from '../../testing/create-fake-admin';
import { createFakeCommandContext } from '../../testing/create-command-context';
import { configDescribeCommand } from './describe';

function fakeDescribeConfigsResult(overrides: Partial<Record<string, unknown>> = {}) {
  return {
    resources: [
      {
        errorCode: 0,
        errorMessage: null,
        resourceType: 2,
        resourceName: 'orders',
        configEntries: [
          {
            configName: 'retention.ms',
            configValue: '604800000',
            readOnly: false,
            isDefault: false,
            configSource: 1,
            isSensitive: false,
            configSynonyms: [],
          },
        ],
        ...overrides,
      },
    ],
  };
}

describe('configDescribeCommand', () => {
  it('describes a single resource', async () => {
    const describeConfigs = vi.fn(async () => fakeDescribeConfigsResult());
    const admin = createFakeAdmin({ describeConfigs, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', type: 'topic' },
      positionals: ['orders'],
      openAdmin: async () => admin,
    });

    const code = await configDescribeCommand.run(context);

    expect(code).toBe(0);
    expect(describeConfigs).toHaveBeenCalledWith({
      resources: [{ type: 2, name: 'orders', configNames: undefined }],
      includeSynonyms: false,
      includeDocumentation: false,
    });
  });

  it('passes --config-name as the resource configNames filter', async () => {
    const describeConfigs = vi.fn(async () => fakeDescribeConfigsResult());
    const admin = createFakeAdmin({ describeConfigs, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', type: 'topic', 'config-name': ['retention.ms', 'cleanup.policy'] },
      positionals: ['orders'],
      openAdmin: async () => admin,
    });

    await configDescribeCommand.run(context);
    expect(describeConfigs).toHaveBeenCalledWith(
      expect.objectContaining({
        resources: [expect.objectContaining({ configNames: ['retention.ms', 'cleanup.policy'] })],
      }),
    );
  });

  it('forwards --include-synonyms and --include-documentation', async () => {
    const describeConfigs = vi.fn(async () => fakeDescribeConfigsResult());
    const admin = createFakeAdmin({ describeConfigs, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: {
        brokers: 'localhost:9092',
        type: 'topic',
        'include-synonyms': true,
        'include-documentation': true,
      },
      positionals: ['orders'],
      openAdmin: async () => admin,
    });

    await configDescribeCommand.run(context);
    expect(describeConfigs).toHaveBeenCalledWith(
      expect.objectContaining({ includeSynonyms: true, includeDocumentation: true }),
    );
  });

  it('redacts a sensitive entry value by default', async () => {
    const describeConfigs = vi.fn(async () => ({
      resources: [
        {
          errorCode: 0,
          errorMessage: null,
          resourceType: 2,
          resourceName: 'orders',
          configEntries: [
            {
              configName: 'sasl.jaas.config',
              configValue: 'super-secret',
              readOnly: false,
              isDefault: false,
              configSource: 1,
              isSensitive: true,
              configSynonyms: [],
            },
          ],
        },
      ],
    }));
    const admin = createFakeAdmin({ describeConfigs, disconnect: async () => {} });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', type: 'topic' },
      positionals: ['orders'],
      openAdmin: async () => admin,
      format: 'json',
    });

    await configDescribeCommand.run(context);
    const written = JSON.parse(stdoutWrite.mock.calls[0]![0]) as {
      resources: { entries: { value: string | null }[] }[];
    };
    expect(written.resources[0]!.entries[0]!.value).toBe('[REDACTED]');
  });

  it('--show-secrets prints a sensitive entry value unredacted', async () => {
    const describeConfigs = vi.fn(async () => ({
      resources: [
        {
          errorCode: 0,
          errorMessage: null,
          resourceType: 2,
          resourceName: 'orders',
          configEntries: [
            {
              configName: 'sasl.jaas.config',
              configValue: 'super-secret',
              readOnly: false,
              isDefault: false,
              configSource: 1,
              isSensitive: true,
              configSynonyms: [],
            },
          ],
        },
      ],
    }));
    const admin = createFakeAdmin({ describeConfigs, disconnect: async () => {} });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', type: 'topic', 'show-secrets': true },
      positionals: ['orders'],
      openAdmin: async () => admin,
      format: 'json',
    });

    await configDescribeCommand.run(context);
    const written = JSON.parse(stdoutWrite.mock.calls[0]![0]) as {
      resources: { entries: { value: string | null }[] }[];
    };
    expect(written.resources[0]!.entries[0]!.value).toBe('super-secret');
  });

  it('requires at least one resource name', async () => {
    const { context } = createFakeCommandContext({ flags: { type: 'topic' }, positionals: [] });
    await expect(configDescribeCommand.run(context)).rejects.toThrow(/at least one resource name/);
  });

  it('requires --type', async () => {
    const { context } = createFakeCommandContext({ flags: {}, positionals: ['orders'] });
    await expect(configDescribeCommand.run(context)).rejects.toThrow(/--type/);
  });

  it('rejects an unknown --type', async () => {
    const { context } = createFakeCommandContext({ flags: { type: 'bogus' }, positionals: ['orders'] });
    await expect(configDescribeCommand.run(context)).rejects.toThrow(CliUsageError);
  });

  it('fans out one call per resource when more than one name is given', async () => {
    const describeConfigs = vi.fn(async () => fakeDescribeConfigsResult());
    const admin = createFakeAdmin({ describeConfigs, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', type: 'topic' },
      positionals: ['orders', 'payments'],
      openAdmin: async () => admin,
    });

    const code = await configDescribeCommand.run(context);

    expect(code).toBe(0);
    expect(describeConfigs).toHaveBeenCalledTimes(2);
  });

  it('returns exit 4 on a fanned-out partial failure', async () => {
    const describeConfigs = vi.fn(async ({ resources }: { resources: { name: string }[] }) => {
      if (resources[0]!.name === 'payments') throw new Error('boom');
      return fakeDescribeConfigsResult();
    });
    const admin = createFakeAdmin({ describeConfigs, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', type: 'topic' },
      positionals: ['orders', 'payments'],
      openAdmin: async () => admin,
    });

    const code = await configDescribeCommand.run(context);
    expect(code).toBe(4);
  });

  it('returns exit 1 when every fanned-out call fails', async () => {
    const describeConfigs = vi.fn(async () => {
      throw new Error('boom');
    });
    const admin = createFakeAdmin({ describeConfigs, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', type: 'topic' },
      positionals: ['orders', 'payments'],
      openAdmin: async () => admin,
    });

    const code = await configDescribeCommand.run(context);
    expect(code).toBe(1);
  });

  it('reports json output with the raw config entries', async () => {
    const describeConfigs = vi.fn(async () => fakeDescribeConfigsResult());
    const admin = createFakeAdmin({ describeConfigs, disconnect: async () => {} });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', type: 'topic' },
      positionals: ['orders'],
      openAdmin: async () => admin,
      format: 'json',
    });

    await configDescribeCommand.run(context);
    const written = JSON.parse(stdoutWrite.mock.calls[0]![0]) as {
      resources: { resource: string; ok: boolean; entries: { name: string; source: string }[] }[];
    };
    expect(written.resources[0]!.resource).toBe('orders');
    expect(written.resources[0]!.ok).toBe(true);
    expect(written.resources[0]!.entries[0]!.name).toBe('retention.ms');
    expect(written.resources[0]!.entries[0]!.source).toBe('TOPIC_CONFIG');
  });

  it('disconnects even when a single-resource call throws', async () => {
    const disconnect = vi.fn(async () => {});
    const admin = createFakeAdmin({
      describeConfigs: async () => {
        throw new Error('boom');
      },
      disconnect,
    });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', type: 'topic' },
      positionals: ['orders'],
      openAdmin: async () => admin,
    });

    await expect(configDescribeCommand.run(context)).rejects.toThrow('boom');
    expect(disconnect).toHaveBeenCalledTimes(1);
  });
});
