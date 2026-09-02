import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { CliUsageError } from '../../args/coerce';
import { CliAbortedError } from '../../errors/aborted-error';
import { createFakeAdmin } from '../../testing/create-fake-admin';
import { createFakeCommandContext } from '../../testing/create-command-context';
import { clusterElectLeadersCommand } from './elect-leaders';

function fakeElectResult(errorCode = 0) {
  return { results: [{ topic: 'orders', partitions: [{ partition: 0, errorCode, errorMessage: null }] }] };
}

let tmpDir: string | undefined;

afterEach(() => {
  if (tmpDir !== undefined) {
    rmSync(tmpDir, { recursive: true, force: true });
    tmpDir = undefined;
  }
});

describe('clusterElectLeadersCommand', () => {
  it('requires --election-type', async () => {
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', 'all-topic-partitions': true },
    });

    await expect(clusterElectLeadersCommand.run(context)).rejects.toThrow(/requires --election-type/);
  });

  it('requires exactly one of --topic-partition, --all-topic-partitions, or --from-file', async () => {
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', 'election-type': 'preferred' },
    });
    await expect(clusterElectLeadersCommand.run(context)).rejects.toThrow(CliUsageError);

    const { context: both } = createFakeCommandContext({
      flags: {
        brokers: 'localhost:9092',
        'election-type': 'preferred',
        'all-topic-partitions': true,
        'topic-partition': ['orders:0'],
      },
    });
    await expect(clusterElectLeadersCommand.run(both)).rejects.toThrow(CliUsageError);
  });

  it('rejects a malformed --topic-partition', async () => {
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', 'election-type': 'preferred', 'topic-partition': ['orders'] },
    });

    await expect(clusterElectLeadersCommand.run(context)).rejects.toThrow(CliUsageError);
  });

  it('aborts without --yes off a TTY', async () => {
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', 'election-type': 'preferred', 'all-topic-partitions': true },
    });

    await expect(clusterElectLeadersCommand.run(context)).rejects.toThrow(CliAbortedError);
  });

  it('groups --topic-partition entries by topic', async () => {
    const electLeaders = vi.fn(async () => fakeElectResult());
    const admin = createFakeAdmin({ electLeaders, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: {
        brokers: 'localhost:9092',
        'election-type': 'preferred',
        'topic-partition': ['orders:0', 'orders:1'],
        yes: true,
      },
      openAdmin: async () => admin,
    });

    await clusterElectLeadersCommand.run(context);

    expect(electLeaders).toHaveBeenCalledWith({
      topicPartitions: [{ topic: 'orders', partitions: [0, 1] }],
      electionType: 0,
      timeout: undefined,
    });
  });

  it('passes topicPartitions: null for --all-topic-partitions', async () => {
    const electLeaders = vi.fn(async () => fakeElectResult());
    const admin = createFakeAdmin({ electLeaders, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', 'election-type': 'preferred', 'all-topic-partitions': true, yes: true },
      openAdmin: async () => admin,
    });

    await clusterElectLeadersCommand.run(context);

    expect(electLeaders).toHaveBeenCalledWith({ topicPartitions: null, electionType: 0, timeout: undefined });
  });

  it('reads --from-file in the kafka-leader-election.sh shape', async () => {
    tmpDir = mkdtempSync(join(tmpdir(), 'kafka-cli-elect-leaders-'));
    const file = join(tmpDir, 'election.json');
    writeFileSync(file, JSON.stringify({ partitions: [{ topic: 'orders', partition: 0 }] }));

    const electLeaders = vi.fn(async () => fakeElectResult());
    const admin = createFakeAdmin({ electLeaders, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', 'election-type': 'preferred', 'from-file': file, yes: true },
      openAdmin: async () => admin,
    });

    await clusterElectLeadersCommand.run(context);

    expect(electLeaders).toHaveBeenCalledWith({
      topicPartitions: [{ topic: 'orders', partitions: [0] }],
      electionType: 0,
      timeout: undefined,
    });
  });

  it('--dry-run prints the target and never opens an admin connection', async () => {
    const openAdmin = vi.fn();
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: {
        brokers: 'localhost:9092',
        'election-type': 'preferred',
        'topic-partition': ['orders:0'],
        'dry-run': true,
      },
      openAdmin,
    });

    const code = await clusterElectLeadersCommand.run(context);

    expect(code).toBe(0);
    expect(openAdmin).not.toHaveBeenCalled();
    expect(stdoutWrite.mock.calls[0]![0]).toContain('orders:0');
  });

  it('--dry-run does not require --force even for an unclean election', async () => {
    const openAdmin = vi.fn();
    const { context } = createFakeCommandContext({
      flags: {
        brokers: 'localhost:9092',
        'election-type': 'unclean',
        'all-topic-partitions': true,
        'dry-run': true,
      },
      openAdmin,
    });

    const code = await clusterElectLeadersCommand.run(context);
    expect(code).toBe(0);
  });

  it('requires --force for an unclean election on a real run, even with --yes', async () => {
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', 'election-type': 'unclean', 'all-topic-partitions': true, yes: true },
    });

    await expect(clusterElectLeadersCommand.run(context)).rejects.toThrow(CliAbortedError);
  });

  it('still requires --yes for an unclean election once --force is given', async () => {
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', 'election-type': 'unclean', 'all-topic-partitions': true, force: true },
    });

    await expect(clusterElectLeadersCommand.run(context)).rejects.toThrow(CliAbortedError);
  });

  it('runs an unclean election once --force and --yes are given', async () => {
    const electLeaders = vi.fn(async () => fakeElectResult());
    const admin = createFakeAdmin({ electLeaders, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: {
        brokers: 'localhost:9092',
        'election-type': 'unclean',
        'all-topic-partitions': true,
        force: true,
        yes: true,
      },
      openAdmin: async () => admin,
    });

    const code = await clusterElectLeadersCommand.run(context);
    expect(code).toBe(0);
    expect(electLeaders).toHaveBeenCalledWith({ topicPartitions: null, electionType: 1, timeout: undefined });
  });

  it('renders "elected" and "not needed" per partition status', async () => {
    const electLeaders = vi.fn(async () => ({
      results: [
        {
          topic: 'orders',
          partitions: [
            { partition: 0, errorCode: 0, errorMessage: null },
            { partition: 1, errorCode: 84, errorMessage: null },
          ],
        },
      ],
    }));
    const admin = createFakeAdmin({ electLeaders, disconnect: async () => {} });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', 'election-type': 'preferred', 'all-topic-partitions': true, yes: true },
      openAdmin: async () => admin,
    });

    await clusterElectLeadersCommand.run(context);

    const written = stdoutWrite.mock.calls[0]![0];
    expect(written).toContain('elected');
    expect(written).toContain('not needed');
  });

  it('reports json output with the raw results shape', async () => {
    const electLeaders = vi.fn(async () => fakeElectResult());
    const admin = createFakeAdmin({ electLeaders, disconnect: async () => {} });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', 'election-type': 'preferred', 'all-topic-partitions': true, yes: true },
      openAdmin: async () => admin,
      format: 'json',
    });

    await clusterElectLeadersCommand.run(context);

    const written = JSON.parse(stdoutWrite.mock.calls[0]![0]) as ReturnType<typeof fakeElectResult>;
    expect(written).toEqual(fakeElectResult());
  });

  it('disconnects even when electLeaders throws', async () => {
    const disconnect = vi.fn(async () => {});
    const admin = createFakeAdmin({
      electLeaders: async () => {
        throw new Error('boom');
      },
      disconnect,
    });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', 'election-type': 'preferred', 'all-topic-partitions': true, yes: true },
      openAdmin: async () => admin,
    });

    await expect(clusterElectLeadersCommand.run(context)).rejects.toThrow('boom');
    expect(disconnect).toHaveBeenCalledTimes(1);
  });
});
