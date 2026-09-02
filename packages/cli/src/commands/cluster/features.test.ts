import { describe, expect, it } from 'vitest';
import { createFakeAdmin } from '../../testing/create-fake-admin';
import { createFakeCommandContext } from '../../testing/create-command-context';
import { clusterFeaturesCommand } from './features';

function fakeFeatures() {
  return {
    supportedFeatures: [{ name: 'kraft.version', minVersion: 0, maxVersion: 1 }],
    finalizedFeatures: [{ name: 'kraft.version', maxVersionLevel: 1, minVersionLevel: 0 }],
    finalizedFeaturesEpoch: 5n,
    zkMigrationReady: false,
  };
}

describe('clusterFeaturesCommand', () => {
  it('merges supported and finalized features by name', async () => {
    const describeFeatures = async () => fakeFeatures();
    const admin = createFakeAdmin({ describeFeatures, disconnect: async () => {} });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      openAdmin: async () => admin,
    });

    const code = await clusterFeaturesCommand.run(context);

    expect(code).toBe(0);
    const written = stdoutWrite.mock.calls[0]![0];
    expect(written).toContain('kraft.version');
    expect(written).toContain('0-1');
    expect(written).toContain('Finalized features epoch: 5');
  });

  it('renders "(unsupported)"/"(not finalized)" for a one-sided feature', async () => {
    const describeFeatures = async () => ({
      supportedFeatures: [{ name: 'only.supported', minVersion: 0, maxVersion: 2 }],
      finalizedFeatures: [{ name: 'only.finalized', maxVersionLevel: 1, minVersionLevel: 1 }],
      finalizedFeaturesEpoch: 0n,
      zkMigrationReady: null,
    });
    const admin = createFakeAdmin({ describeFeatures, disconnect: async () => {} });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      openAdmin: async () => admin,
    });

    await clusterFeaturesCommand.run(context);

    const written = stdoutWrite.mock.calls[0]![0];
    expect(written).toContain('(not finalized)');
    expect(written).toContain('(unsupported)');
    expect(written).toContain('ZK migration ready: (n/a)');
  });

  it('reports json output with the epoch bigint encoded as a string', async () => {
    const describeFeatures = async () => fakeFeatures();
    const admin = createFakeAdmin({ describeFeatures, disconnect: async () => {} });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      openAdmin: async () => admin,
      format: 'json',
    });

    await clusterFeaturesCommand.run(context);

    const written = JSON.parse(stdoutWrite.mock.calls[0]![0]) as { finalizedFeaturesEpoch: string };
    expect(written.finalizedFeaturesEpoch).toBe('5');
  });
});
