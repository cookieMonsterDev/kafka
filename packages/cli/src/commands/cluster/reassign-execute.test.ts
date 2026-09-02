import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { CliUsageError } from '../../args/coerce';
import { CliAbortedError } from '../../errors/aborted-error';
import { createFakeAdmin } from '../../testing/create-fake-admin';
import { createFakeCommandContext } from '../../testing/create-command-context';
import { clusterReassignExecuteCommand } from './reassign-execute';

function fakeAggregateError(failures: { topic: string; partition: number; message: string }[]) {
  const error = new Error('Errors altering partition reassignments') as Error & { name: string; errors: unknown[] };
  error.name = 'KafkaAggregateError';
  error.errors = failures.map((failure) => {
    const item = new Error(failure.message) as Error & { name: string; topic: string; partition: number };
    item.name = 'KafkaAlterPartitionReassignmentsError';
    item.topic = failure.topic;
    item.partition = failure.partition;
    return item;
  });
  return error;
}

let tmpDir: string | undefined;

afterEach(() => {
  if (tmpDir !== undefined) {
    rmSync(tmpDir, { recursive: true, force: true });
    tmpDir = undefined;
  }
});

function writeReassignmentFile(content: unknown): string {
  tmpDir = mkdtempSync(join(tmpdir(), 'kafka-cli-reassign-'));
  const file = join(tmpDir, 'reassignment.json');
  writeFileSync(file, JSON.stringify(content));
  return file;
}

describe('clusterReassignExecuteCommand', () => {
  it('requires --from-file', async () => {
    const { context } = createFakeCommandContext({ flags: { brokers: 'localhost:9092', yes: true } });
    await expect(clusterReassignExecuteCommand.run(context)).rejects.toThrow(/requires --from-file/);
  });

  it('rejects a malformed reassignment file', async () => {
    const file = writeReassignmentFile({ partitions: [{ topic: 'orders' }] });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', 'from-file': file, yes: true },
    });

    await expect(clusterReassignExecuteCommand.run(context)).rejects.toThrow(CliUsageError);
  });

  it('aborts without --yes off a TTY', async () => {
    const file = writeReassignmentFile({ partitions: [{ topic: 'orders', partition: 0, replicas: [1, 2] }] });
    const { context } = createFakeCommandContext({ flags: { brokers: 'localhost:9092', 'from-file': file } });

    await expect(clusterReassignExecuteCommand.run(context)).rejects.toThrow(CliAbortedError);
  });

  it('--dry-run prints the plan and never opens an admin connection', async () => {
    const file = writeReassignmentFile({ partitions: [{ topic: 'orders', partition: 0, replicas: [1, 2] }] });
    const openAdmin = vi.fn();
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', 'from-file': file, 'dry-run': true },
      openAdmin,
    });

    const code = await clusterReassignExecuteCommand.run(context);

    expect(code).toBe(0);
    expect(openAdmin).not.toHaveBeenCalled();
    const written = stdoutWrite.mock.calls[0]![0];
    expect(written).toContain('orders');
    expect(written).toContain('1, 2');
  });

  it('--dry-run notes that log_dirs entries are ignored', async () => {
    const file = writeReassignmentFile({
      partitions: [{ topic: 'orders', partition: 0, replicas: [1, 2], log_dirs: ['any', 'any'] }],
    });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', 'from-file': file, 'dry-run': true },
    });

    await clusterReassignExecuteCommand.run(context);

    expect(stdoutWrite.mock.calls[0]![0]).toContain('log_dirs');
  });

  it('submits the reassignment and reports success', async () => {
    const file = writeReassignmentFile({ partitions: [{ topic: 'orders', partition: 0, replicas: [1, 2] }] });
    const alterPartitionReassignments = vi.fn(async () => {});
    const admin = createFakeAdmin({ alterPartitionReassignments, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', 'from-file': file, yes: true },
      openAdmin: async () => admin,
    });

    const code = await clusterReassignExecuteCommand.run(context);

    expect(code).toBe(0);
    expect(alterPartitionReassignments).toHaveBeenCalledWith({
      topics: [{ topic: 'orders', partitionAssignment: [{ partition: 0, replicas: [1, 2] }] }],
      timeout: undefined,
    });
  });

  it('reports a partial batch when only some partitions fail', async () => {
    const file = writeReassignmentFile({
      partitions: [
        { topic: 'orders', partition: 0, replicas: [1, 2] },
        { topic: 'orders', partition: 1, replicas: [1, 2] },
      ],
    });
    const alterPartitionReassignments = vi.fn(async () => {
      throw fakeAggregateError([{ topic: 'orders', partition: 1, message: 'invalid replica' }]);
    });
    const admin = createFakeAdmin({ alterPartitionReassignments, disconnect: async () => {} });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', 'from-file': file, yes: true },
      openAdmin: async () => admin,
    });

    const code = await clusterReassignExecuteCommand.run(context);

    expect(code).toBe(4);
    expect(stdoutWrite.mock.calls[0]![0]).toContain('invalid replica');
  });

  it('returns operationFailed when every partition fails', async () => {
    const file = writeReassignmentFile({ partitions: [{ topic: 'orders', partition: 0, replicas: [1, 2] }] });
    const alterPartitionReassignments = vi.fn(async () => {
      throw fakeAggregateError([{ topic: 'orders', partition: 0, message: 'bad' }]);
    });
    const admin = createFakeAdmin({ alterPartitionReassignments, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', 'from-file': file, yes: true },
      openAdmin: async () => admin,
    });

    const code = await clusterReassignExecuteCommand.run(context);
    expect(code).toBe(1);
  });

  it('rethrows a non-aggregate error unchanged', async () => {
    const file = writeReassignmentFile({ partitions: [{ topic: 'orders', partition: 0, replicas: [1, 2] }] });
    const alterPartitionReassignments = vi.fn(async () => {
      throw new Error('request-level failure');
    });
    const admin = createFakeAdmin({ alterPartitionReassignments, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', 'from-file': file, yes: true },
      openAdmin: async () => admin,
    });

    await expect(clusterReassignExecuteCommand.run(context)).rejects.toThrow('request-level failure');
  });

  it('disconnects even when alterPartitionReassignments throws', async () => {
    const file = writeReassignmentFile({ partitions: [{ topic: 'orders', partition: 0, replicas: [1, 2] }] });
    const disconnect = vi.fn(async () => {});
    const admin = createFakeAdmin({
      alterPartitionReassignments: async () => {
        throw new Error('boom');
      },
      disconnect,
    });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', 'from-file': file, yes: true },
      openAdmin: async () => admin,
    });

    await expect(clusterReassignExecuteCommand.run(context)).rejects.toThrow('boom');
    expect(disconnect).toHaveBeenCalledTimes(1);
  });
});
