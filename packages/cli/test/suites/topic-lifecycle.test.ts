import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { runCli } from '../helpers/run-cli';

const BROKERS = process.env.KAFKA_BROKERS ?? 'localhost:9092';
const TOPIC = `kafka-cli-it-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;

/**
 * Runs against a real broker started by `test/helpers/global-setup.ts`. Adapted from the
 * originally scoped "create -> list -> describe -> delete" shape in two ways, both because this
 * track's command surface doesn't cover them yet: there is no `topic delete` command (a later
 * track's job), so this suite stops at describe; and `ping`/`admin call` connect over a plain
 * broker rather than a SASL one, since the runtime's admin port doesn't accept SASL options yet
 * (that lands once the CLI reads a config file). Both gaps are expected to close in later tracks
 * — re-tighten this suite's scope when they do.
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
