import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { createFakeAdmin } from '../../testing/create-fake-admin';
import { createFakeCommandContext, EMPTY_RESOLVED_CLI_CONFIG } from '../../testing/create-command-context';
import { topicDeleteRecordsCommand } from './delete-records';

describe('topicDeleteRecordsCommand', () => {
  let dir: string;

  beforeEach(() => {
    dir = mkdtempSync(join(tmpdir(), 'kafka-cli-delete-records-'));
  });

  afterEach(() => {
    rmSync(dir, { recursive: true, force: true });
  });

  function writeOffsetsFile(content: unknown): string {
    const path = join(dir, 'offsets.json');
    writeFileSync(path, typeof content === 'string' ? content : JSON.stringify(content));
    return path;
  }

  it('requires --from-file', async () => {
    const { context } = createFakeCommandContext({ flags: { brokers: 'localhost:9092', yes: true }, positionals: [] });
    await expect(topicDeleteRecordsCommand.run(context)).rejects.toThrow(/--from-file/);
  });

  it('rejects a from-file path that does not exist', async () => {
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', yes: true, 'from-file': join(dir, 'missing.json') },
      positionals: [],
    });
    await expect(topicDeleteRecordsCommand.run(context)).rejects.toThrow(/could not read/);
  });

  it('rejects invalid JSON', async () => {
    const path = writeOffsetsFile('not json');
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', yes: true, 'from-file': path },
      positionals: [],
    });
    await expect(topicDeleteRecordsCommand.run(context)).rejects.toThrow(/not valid JSON/);
  });

  it('rejects a file without a "partitions" array', async () => {
    const path = writeOffsetsFile({ version: 1 });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', yes: true, 'from-file': path },
      positionals: [],
    });
    await expect(topicDeleteRecordsCommand.run(context)).rejects.toThrow(/"partitions" array/);
  });

  it('rejects a malformed partition entry', async () => {
    const path = writeOffsetsFile({ partitions: [{ topic: 'orders', partition: 'zero', offset: 3 }] });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', yes: true, 'from-file': path },
      positionals: [],
    });
    await expect(topicDeleteRecordsCommand.run(context)).rejects.toThrow(/partitions\[0\]/);
  });

  it('rejects an empty partitions list', async () => {
    const path = writeOffsetsFile({ partitions: [] });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', yes: true, 'from-file': path },
      positionals: [],
    });
    await expect(topicDeleteRecordsCommand.run(context)).rejects.toThrow(/lists no partitions/);
  });

  it('aborts without --yes off a TTY', async () => {
    const path = writeOffsetsFile({ partitions: [{ topic: 'orders', partition: 0, offset: 3 }] });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', 'from-file': path },
      positionals: [],
    });
    await expect(topicDeleteRecordsCommand.run(context)).rejects.toThrow(/--yes/);
  });

  it('skips confirmation when cli.confirmDestructive is false', async () => {
    const path = writeOffsetsFile({ partitions: [{ topic: 'orders', partition: 0, offset: 3 }] });
    const deleteTopicRecords = vi.fn(async () => {});
    const admin = createFakeAdmin({ deleteTopicRecords, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', 'from-file': path },
      positionals: [],
      openAdmin: async () => admin,
      config: { ...EMPTY_RESOLVED_CLI_CONFIG, cli: { confirmDestructive: false } },
    });

    const code = await topicDeleteRecordsCommand.run(context);
    expect(code).toBe(0);
  });

  it("groups a single topic's partitions into one call", async () => {
    const path = writeOffsetsFile({
      partitions: [
        { topic: 'orders', partition: 0, offset: 3 },
        { topic: 'orders', partition: 1, offset: 5 },
      ],
    });
    const deleteTopicRecords = vi.fn(async () => {});
    const admin = createFakeAdmin({ deleteTopicRecords, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', yes: true, 'from-file': path },
      positionals: [],
      openAdmin: async () => admin,
    });

    const code = await topicDeleteRecordsCommand.run(context);

    expect(code).toBe(0);
    expect(deleteTopicRecords).toHaveBeenCalledTimes(1);
    expect(deleteTopicRecords).toHaveBeenCalledWith({
      topic: 'orders',
      partitions: [
        { partition: 0, offset: 3 },
        { partition: 1, offset: 5 },
      ],
    });
  });

  it('fans out one call per topic when the file spans multiple topics', async () => {
    const path = writeOffsetsFile({
      partitions: [
        { topic: 'orders', partition: 0, offset: 3 },
        { topic: 'payments', partition: 0, offset: 1 },
      ],
    });
    const deleteTopicRecords = vi.fn(async () => {});
    const admin = createFakeAdmin({ deleteTopicRecords, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', yes: true, 'from-file': path },
      positionals: [],
      openAdmin: async () => admin,
    });

    const code = await topicDeleteRecordsCommand.run(context);

    expect(code).toBe(0);
    expect(deleteTopicRecords).toHaveBeenCalledTimes(2);
    expect(deleteTopicRecords).toHaveBeenCalledWith({ topic: 'orders', partitions: [{ partition: 0, offset: 3 }] });
    expect(deleteTopicRecords).toHaveBeenCalledWith({ topic: 'payments', partitions: [{ partition: 0, offset: 1 }] });
  });

  it('returns exit 4 on a fanned-out partial failure', async () => {
    const path = writeOffsetsFile({
      partitions: [
        { topic: 'orders', partition: 0, offset: 3 },
        { topic: 'payments', partition: 0, offset: 1 },
      ],
    });
    const deleteTopicRecords = vi.fn(async ({ topic }: { topic: string }) => {
      if (topic === 'payments') throw new Error('boom');
    });
    const admin = createFakeAdmin({ deleteTopicRecords, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', yes: true, 'from-file': path },
      positionals: [],
      openAdmin: async () => admin,
    });

    const code = await topicDeleteRecordsCommand.run(context);
    expect(code).toBe(4);
  });

  it('accepts a string offset, matching kafka-delete-records.sh', async () => {
    const path = writeOffsetsFile({ partitions: [{ topic: 'orders', partition: 0, offset: '9007199254740993' }] });
    const deleteTopicRecords = vi.fn(async () => {});
    const admin = createFakeAdmin({ deleteTopicRecords, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', yes: true, 'from-file': path },
      positionals: [],
      openAdmin: async () => admin,
    });

    await topicDeleteRecordsCommand.run(context);
    expect(deleteTopicRecords).toHaveBeenCalledWith({
      topic: 'orders',
      partitions: [{ partition: 0, offset: '9007199254740993' }],
    });
  });

  it('disconnects even when a single-topic call throws', async () => {
    const path = writeOffsetsFile({ partitions: [{ topic: 'orders', partition: 0, offset: 3 }] });
    const disconnect = vi.fn(async () => {});
    const admin = createFakeAdmin({
      deleteTopicRecords: async () => {
        throw new Error('boom');
      },
      disconnect,
    });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', yes: true, 'from-file': path },
      positionals: [],
      openAdmin: async () => admin,
    });

    await expect(topicDeleteRecordsCommand.run(context)).rejects.toThrow('boom');
    expect(disconnect).toHaveBeenCalledTimes(1);
  });
});
