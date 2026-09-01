import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { runCli } from '../helpers/run-cli';

const BROKERS = process.env.KAFKA_BROKERS ?? 'localhost:9092';
const TOPIC = `kafka-cli-it-group-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;

/**
 * Runs against a real broker started by `test/helpers/global-setup.ts` — same disclosed gap as
 * `topic-lifecycle.test.ts` and `config-lifecycle.test.ts` (no SASL support in the runtime's admin
 * port yet). Kept small: this only checks this CLI's own wiring (flag parsing, option shapes, exit
 * codes), not group-membership behavior core's own test suite already covers end to end.
 */
describe('group lifecycle against a real broker', () => {
  beforeAll(async () => {
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
    if (created.code !== 0) {
      throw new Error(`setup: topic create failed with exit ${created.code}: ${created.stderr}`);
    }
  });

  afterAll(async () => {
    await runCli(['topic', 'delete', TOPIC, '--brokers', BROKERS, '--yes']);
  });

  it('lists groups (empty or not) without error', async () => {
    const result = await runCli(['group', 'list', '--brokers', BROKERS, '--json']);
    expect(result.code).toBe(0);
    const parsed = JSON.parse(result.stdout) as { groups: unknown[] };
    expect(Array.isArray(parsed.groups)).toBe(true);
  });

  it('exits non-zero describing a group that has never existed', async () => {
    const result = await runCli(['group', 'describe', 'kafka-cli-it-nonexistent-group', '--brokers', BROKERS]);
    expect(result.code).not.toBe(0);
  });

  it('previews a reset-offsets dry run against a topic with no committed offsets', async () => {
    const result = await runCli([
      'group',
      'reset-offsets',
      'kafka-cli-it-nonexistent-group',
      '--topic',
      TOPIC,
      '--to',
      'earliest',
      '--brokers',
      BROKERS,
      '--json',
    ]);
    expect(result.code).toBe(0);
    const parsed = JSON.parse(result.stdout) as { topics: { topic: string; ok: boolean }[] };
    expect(parsed.topics[0]?.topic).toBe(TOPIC);
    expect(parsed.topics[0]?.ok).toBe(true);
  });
});
