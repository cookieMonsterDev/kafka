import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { runCli } from '../helpers/run-cli';
import { waitFor } from '../helpers/wait-for';

const BROKERS = process.env.KAFKA_BROKERS ?? 'localhost:9092';
const TOPIC = `kafka-cli-it-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;

/**
 * Runs against a real broker started by `test/helpers/global-setup.ts`. Adapted from the
 * originally scoped "create -> list -> describe -> delete" shape in one remaining way:
 * `ping`/`admin call` connect over a plain broker rather than a SASL one, since the runtime's
 * admin port doesn't accept SASL options yet (that lands once the CLI reads a config file).
 * Re-tighten this suite's scope once that gap closes. `topic delete` and the other mutation
 * commands (`add-partitions`, `offsets`, `delete-records`, `producers`) are covered below, in
 * their own describe block against their own topic.
 */
describe('topic lifecycle against a real broker', () => {
  let argsFileDir: string;

  beforeAll(() => {
    argsFileDir = mkdtempSync(join(tmpdir(), 'kafka-cli-it-'));
  });

  afterAll(() => {
    rmSync(argsFileDir, { recursive: true, force: true });
  });

  it('pings the cluster', async () => {
    const result = await runCli(['ping', '--brokers', BROKERS]);
    expect(result.code).toBe(0);
    expect(result.stdout).toContain('ok');
  });

  it('--dry-run creates nothing', async () => {
    const before = await runCli(['topic', 'list', '--brokers', BROKERS, '--json']);
    const beforeTopics = (JSON.parse(before.stdout) as { topics: string[] }).topics;
    expect(beforeTopics).not.toContain(TOPIC);

    const dryRun = await runCli([
      'topic',
      'create',
      TOPIC,
      '--brokers',
      BROKERS,
      '--partitions',
      '1',
      '--replication-factor',
      '1',
      '--dry-run',
    ]);
    expect(dryRun.code).toBe(0);

    const after = await runCli(['topic', 'list', '--brokers', BROKERS, '--json']);
    const afterTopics = (JSON.parse(after.stdout) as { topics: string[] }).topics;
    expect(afterTopics).not.toContain(TOPIC);
  });

  it('creates the topic, then lists and describes it', async () => {
    const created = await runCli([
      'topic',
      'create',
      TOPIC,
      '--brokers',
      BROKERS,
      '--partitions',
      '1',
      '--replication-factor',
      '1',
    ]);
    expect(created.code).toBe(0);

    const listed = await runCli(['topic', 'list', '--brokers', BROKERS, '--json']);
    expect((JSON.parse(listed.stdout) as { topics: string[] }).topics).toContain(TOPIC);

    const described = await runCli(['topic', 'describe', TOPIC, '--brokers', BROKERS, '--json']);
    expect(described.code).toBe(0);
    const describedTopics = (JSON.parse(described.stdout) as { topics: { name: string }[] }).topics;
    expect(describedTopics.some((t) => t.name === TOPIC)).toBe(true);
  });

  it('exits 1 on a duplicate create without --if-not-exists', async () => {
    const result = await runCli(['topic', 'create', TOPIC, '--brokers', BROKERS]);
    expect(result.code).toBe(1);
  });

  it('exits 0 on a duplicate create with --if-not-exists', async () => {
    const result = await runCli(['topic', 'create', TOPIC, '--brokers', BROKERS, '--if-not-exists']);
    expect(result.code).toBe(0);
  });

  it('survives a bigint offset through --json via admin call fetchTopicOffsets', async () => {
    const argsPath = join(argsFileDir, 'fetch-topic-offsets.json');
    writeFileSync(argsPath, JSON.stringify(TOPIC));

    const result = await runCli([
      'admin',
      'call',
      'fetchTopicOffsets',
      '--brokers',
      BROKERS,
      '--from-file',
      argsPath,
      '--json',
    ]);

    expect(result.code).toBe(0);
    const offsets = JSON.parse(result.stdout) as { partition: number; offset: string }[];
    expect(offsets.length).toBeGreaterThan(0);
    for (const entry of offsets) {
      expect(typeof entry.offset).toBe('string');
      expect(() => BigInt(entry.offset)).not.toThrow();
    }
  });
});

describe('topic mutations against a real broker', () => {
  const MUTATIONS_TOPIC = `kafka-cli-it-mut-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
  let offsetsFileDir: string;

  beforeAll(async () => {
    offsetsFileDir = mkdtempSync(join(tmpdir(), 'kafka-cli-it-mut-'));
    const created = await runCli([
      'topic',
      'create',
      MUTATIONS_TOPIC,
      '--brokers',
      BROKERS,
      '--partitions',
      '1',
      '--replication-factor',
      '1',
    ]);
    if (created.code !== 0) {
      throw new Error(`setup: topic create failed with exit ${created.code}: ${created.stderr}`);
    }
  });

  afterAll(() => {
    rmSync(offsetsFileDir, { recursive: true, force: true });
  });

  it('raises the topic to a new total partition count', async () => {
    const result = await runCli(['topic', 'add-partitions', MUTATIONS_TOPIC, '--count', '2', '--brokers', BROKERS]);
    expect(result.code).toBe(0);

    // The new partition doesn't always show up in the very next describe — metadata propagation
    // across this suite's multi-broker cluster, not a bug in the add-partitions call itself.
    await waitFor(
      async () => {
        const described = await runCli(['topic', 'describe', MUTATIONS_TOPIC, '--brokers', BROKERS, '--json']);
        if (described.code !== 0) return false;
        const topics = (JSON.parse(described.stdout) as { topics: { partitions: unknown[] }[] }).topics;
        return topics[0]?.partitions.length === 2 ? true : false;
      },
      { message: `${MUTATIONS_TOPIC} to reach 2 partitions` },
    );
  });

  it('reads offsets for the topic', async () => {
    const result = await runCli(['topic', 'offsets', MUTATIONS_TOPIC, '--brokers', BROKERS, '--json']);
    expect(result.code).toBe(0);
    const parsed = JSON.parse(result.stdout) as { partitions: { partition: number; offset: string }[] };
    expect(parsed.partitions.length).toBeGreaterThan(0);
    for (const entry of parsed.partitions) {
      expect(typeof entry.offset).toBe('string');
    }

    const earliest = await runCli([
      'topic',
      'offsets',
      MUTATIONS_TOPIC,
      '--time',
      'earliest',
      '--brokers',
      BROKERS,
      '--json',
    ]);
    expect(earliest.code).toBe(0);
  });

  it('shows producer state for the topic (empty, with no producer connected)', async () => {
    const result = await runCli(['topic', 'producers', MUTATIONS_TOPIC, '--brokers', BROKERS, '--json']);
    expect(result.code).toBe(0);
    const parsed = JSON.parse(result.stdout) as { partitions: unknown[] };
    expect(Array.isArray(parsed.partitions)).toBe(true);
  });

  it('deletes records from the topic via --from-file', async () => {
    const offsetsPath = join(offsetsFileDir, 'delete-records.json');
    writeFileSync(offsetsPath, JSON.stringify({ partitions: [{ topic: MUTATIONS_TOPIC, partition: 0, offset: 0 }] }));

    const result = await runCli(['topic', 'delete-records', '--from-file', offsetsPath, '--brokers', BROKERS, '--yes']);
    expect(result.code).toBe(0);
  });

  it('deletes the topic with --yes', async () => {
    const result = await runCli(['topic', 'delete', MUTATIONS_TOPIC, '--brokers', BROKERS, '--yes']);
    expect(result.code).toBe(0);

    await waitFor(
      async () => {
        const listed = await runCli(['topic', 'list', '--brokers', BROKERS, '--json']);
        if (listed.code !== 0) return false;
        const topics = (JSON.parse(listed.stdout) as { topics: string[] }).topics;
        return topics.includes(MUTATIONS_TOPIC) ? false : true;
      },
      { message: `${MUTATIONS_TOPIC} to disappear from topic list` },
    );
  });
});
