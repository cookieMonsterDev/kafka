import { describe, expect, it, vi } from 'vitest';
import { CliUsageError } from '../../args/coerce';
import { CliAbortedError } from '../../errors/aborted-error';
import { createFakeAdmin } from '../../testing/create-fake-admin';
import { createFakeCommandContext, EMPTY_RESOLVED_CLI_CONFIG } from '../../testing/create-command-context';
import { clusterUpdateFeaturesCommand } from './update-features';

function fakeUpdateFeaturesAggregateError(failures: { feature: string; message: string }[]) {
  const error = new Error('Feature update errors') as Error & { name: string; errors: unknown[] };
  error.name = 'KafkaAggregateError';
  error.errors = failures.map((failure) => {
    const item = new Error(failure.message) as Error & { name: string; feature: string };
    item.name = 'KafkaUpdateFeaturesError';
    item.feature = failure.feature;
    return item;
  });
  return error;
}

describe('clusterUpdateFeaturesCommand', () => {
  it('requires at least one --feature', async () => {
    const openAdmin = vi.fn();
    const { context } = createFakeCommandContext({ flags: { brokers: 'localhost:9092', yes: true }, openAdmin });

    await expect(clusterUpdateFeaturesCommand.run(context)).rejects.toThrow(/at least one --feature/);
    expect(openAdmin).not.toHaveBeenCalled();
  });

  it('aborts without --yes off a TTY', async () => {
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', feature: { 'kraft.version': '1' } },
    });
    await expect(clusterUpdateFeaturesCommand.run(context)).rejects.toThrow(CliAbortedError);
  });

  it('skips confirmation for --dry-run', async () => {
    const updateFeatures = vi.fn(async () => ({ results: [] }));
    const admin = createFakeAdmin({ updateFeatures, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', feature: { 'kraft.version': '1' }, 'dry-run': true },
      openAdmin: async () => admin,
    });

    const code = await clusterUpdateFeaturesCommand.run(context);

    expect(code).toBe(0);
    expect(updateFeatures).toHaveBeenCalledWith({
      featureUpdates: [{ feature: 'kraft.version', maxVersionLevel: 1, upgradeType: 1 }],
      timeout: undefined,
      validateOnly: true,
    });
  });

  it('skips confirmation when cli.confirmDestructive is false', async () => {
    const updateFeatures = vi.fn(async () => ({ results: [] }));
    const admin = createFakeAdmin({ updateFeatures, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', feature: { 'kraft.version': '1' } },
      openAdmin: async () => admin,
      config: { ...EMPTY_RESOLVED_CLI_CONFIG, cli: { confirmDestructive: false } },
    });

    const code = await clusterUpdateFeaturesCommand.run(context);
    expect(code).toBe(0);
  });

  it('requires --force for an unsafe downgrade even with --yes', async () => {
    const { context } = createFakeCommandContext({
      flags: {
        brokers: 'localhost:9092',
        feature: { 'kraft.version': '0' },
        'upgrade-type': 'unsafe-downgrade',
        yes: true,
      },
    });

    await expect(clusterUpdateFeaturesCommand.run(context)).rejects.toThrow(/requires --force/);
  });

  it('resolves --upgrade-type and calls updateFeatures with the expected shape', async () => {
    const updateFeatures = vi.fn(async () => ({ results: [] }));
    const admin = createFakeAdmin({ updateFeatures, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: {
        brokers: 'localhost:9092',
        feature: { 'kraft.version': '0' },
        'upgrade-type': 'unsafe-downgrade',
        yes: true,
        force: true,
        timeout: 5000,
      },
      openAdmin: async () => admin,
    });

    const code = await clusterUpdateFeaturesCommand.run(context);

    expect(code).toBe(0);
    expect(updateFeatures).toHaveBeenCalledWith({
      featureUpdates: [{ feature: 'kraft.version', maxVersionLevel: 0, upgradeType: 3 }],
      timeout: 5000,
      validateOnly: false,
    });
  });

  it('reports a partial batch when only some features fail', async () => {
    const updateFeatures = vi.fn(async () => {
      throw fakeUpdateFeaturesAggregateError([{ feature: 'bad.feature', message: 'invalid feature' }]);
    });
    const admin = createFakeAdmin({ updateFeatures, disconnect: async () => {} });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', feature: { 'kraft.version': '1', 'bad.feature': '9' }, yes: true },
      openAdmin: async () => admin,
    });

    const code = await clusterUpdateFeaturesCommand.run(context);

    expect(code).toBe(4);
    expect(stdoutWrite.mock.calls[0]![0]).toContain('invalid feature');
  });

  it('returns operationFailed when every feature fails', async () => {
    const updateFeatures = vi.fn(async () => {
      throw fakeUpdateFeaturesAggregateError([
        { feature: 'kraft.version', message: 'bad' },
        { feature: 'other.feature', message: 'also bad' },
      ]);
    });
    const admin = createFakeAdmin({ updateFeatures, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', feature: { 'kraft.version': '1', 'other.feature': '1' }, yes: true },
      openAdmin: async () => admin,
    });

    const code = await clusterUpdateFeaturesCommand.run(context);
    expect(code).toBe(1);
  });

  it('rethrows a non-aggregate error unchanged', async () => {
    const updateFeatures = vi.fn(async () => {
      throw new Error('request-level failure');
    });
    const admin = createFakeAdmin({ updateFeatures, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', feature: { 'kraft.version': '1' }, yes: true },
      openAdmin: async () => admin,
    });

    await expect(clusterUpdateFeaturesCommand.run(context)).rejects.toThrow('request-level failure');
  });

  it('rejects a malformed --feature version', async () => {
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', feature: { 'kraft.version': 'not-a-number' }, yes: true },
    });

    await expect(clusterUpdateFeaturesCommand.run(context)).rejects.toThrow(CliUsageError);
  });

  it('disconnects even when updateFeatures throws', async () => {
    const disconnect = vi.fn(async () => {});
    const admin = createFakeAdmin({
      updateFeatures: async () => {
        throw new Error('boom');
      },
      disconnect,
    });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', feature: { 'kraft.version': '1' }, yes: true },
      openAdmin: async () => admin,
    });

    await expect(clusterUpdateFeaturesCommand.run(context)).rejects.toThrow('boom');
    expect(disconnect).toHaveBeenCalledTimes(1);
  });
});
