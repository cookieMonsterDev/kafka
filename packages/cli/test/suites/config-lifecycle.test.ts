import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { runCli } from '../helpers/run-cli';

const BROKERS = process.env.KAFKA_BROKERS ?? 'localhost:9092';
const TOPIC = `kafka-cli-it-config-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;

/**
 * Runs against a real broker started by `test/helpers/global-setup.ts`, over a plain connection —
 * same disclosed gap as `topic-lifecycle.test.ts` (no SASL support in the runtime's admin port yet).
 */
describe('config lifecycle against a real broker', () => {
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

  it('describes the topic default configs', async () => {
    const result = await runCli(['config', 'describe', '--type', 'topic', TOPIC, '--brokers', BROKERS, '--json']);
    expect(result.code).toBe(0);
    const parsed = JSON.parse(result.stdout) as {
      resources: { resource: string; ok: boolean; entries: { name: string }[] }[];
    };
    expect(parsed.resources[0]?.resource).toBe(TOPIC);
    expect(parsed.resources[0]?.ok).toBe(true);
    expect(parsed.resources[0]?.entries.length).toBeGreaterThan(0);
  });

  it('sets a config entry, then reads it back', async () => {
    const set = await runCli([
      'config',
      'set',
      '--type',
      'topic',
      TOPIC,
      '--entry',
      'cleanup.policy=compact',
      '--brokers',
      BROKERS,
    ]);
    expect(set.code).toBe(0);

    const described = await runCli(['config', 'describe', '--type', 'topic', TOPIC, '--brokers', BROKERS, '--json']);
    const parsed = JSON.parse(described.stdout) as {
      resources: { entries: { name: string; value: string | null }[] }[];
    };
    const entry = parsed.resources[0]?.entries.find((e) => e.name === 'cleanup.policy');
    expect(entry?.value).toBe('compact');
  });

  it('unsets the config entry, reverting it to default', async () => {
    const unset = await runCli([
      'config',
      'unset',
      '--type',
      'topic',
      TOPIC,
      '--key',
      'cleanup.policy',
      '--brokers',
      BROKERS,
    ]);
    expect(unset.code).toBe(0);

    const described = await runCli(['config', 'describe', '--type', 'topic', TOPIC, '--brokers', BROKERS, '--json']);
    const parsed = JSON.parse(described.stdout) as {
      resources: { entries: { name: string; isDefault: boolean }[] }[];
    };
    const entry = parsed.resources[0]?.entries.find((e) => e.name === 'cleanup.policy');
    expect(entry?.isDefault).toBe(true);
  });

  it('--dry-run validates a config set without changing anything', async () => {
    const dryRun = await runCli([
      'config',
      'set',
      '--type',
      'topic',
      TOPIC,
      '--entry',
      'retention.ms=1000',
      '--dry-run',
      '--brokers',
      BROKERS,
    ]);
    expect(dryRun.code).toBe(0);

    const described = await runCli(['config', 'describe', '--type', 'topic', TOPIC, '--brokers', BROKERS, '--json']);
    const parsed = JSON.parse(described.stdout) as {
      resources: { entries: { name: string; value: string | null }[] }[];
    };
    const entry = parsed.resources[0]?.entries.find((e) => e.name === 'retention.ms');
    expect(entry?.value).not.toBe('1000');
  });

  it('lists the topic among the topic-type config resources', async () => {
    const result = await runCli(['config', 'list-resources', '--type', 'topic', '--brokers', BROKERS, '--json']);
    expect(result.code).toBe(0);
    const parsed = JSON.parse(result.stdout) as { resources: { resourceName: string }[] };
    expect(parsed.resources.some((r) => r.resourceName === TOPIC)).toBe(true);
  });
});
