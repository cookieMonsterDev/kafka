import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { runCli } from '../helpers/run-cli';

const BROKERS = process.env.KAFKA_BROKERS ?? 'localhost:9092';
const TOPIC = `kafka-cli-it-acl-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
const PRINCIPAL = `User:kafka-cli-it-${Date.now()}`;

/**
 * Runs against a real broker started by `test/helpers/global-setup.ts`, over a plain connection.
 * `KAFKA_SUPER_USERS` in the compose file's broker config includes `User:ANONYMOUS` — the
 * principal an unauthenticated PLAINTEXT connection maps to — so ACL admin calls succeed here
 * even though this suite never authenticates, same disclosed gap as `topic-lifecycle.test.ts`.
 */
describe('acl lifecycle against a real broker', () => {
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
    await runCli([
      'acl',
      'remove',
      PRINCIPAL,
      '--resource-type',
      'topic',
      '--resource-name',
      TOPIC,
      '--brokers',
      BROKERS,
      '--yes',
    ]);
    await runCli(['topic', 'delete', TOPIC, '--brokers', BROKERS, '--yes']);
  });

  it('--dry-run does not create anything', async () => {
    const dryRun = await runCli([
      'acl',
      'add',
      PRINCIPAL,
      '--resource-type',
      'topic',
      '--resource-name',
      TOPIC,
      '--operation',
      'read',
      '--dry-run',
      '--brokers',
      BROKERS,
    ]);
    expect(dryRun.code).toBe(0);

    const listed = await runCli([
      'acl',
      'list',
      '--resource-type',
      'topic',
      '--resource-name',
      TOPIC,
      '--principal',
      PRINCIPAL,
      '--brokers',
      BROKERS,
      '--json',
    ]);
    const parsed = JSON.parse(listed.stdout) as { resources: unknown[] };
    expect(parsed.resources).toEqual([]);
  });

  it('adds an ACL, then lists it back', async () => {
    const added = await runCli([
      'acl',
      'add',
      PRINCIPAL,
      '--resource-type',
      'topic',
      '--resource-name',
      TOPIC,
      '--operation',
      'read',
      '--brokers',
      BROKERS,
    ]);
    expect(added.code).toBe(0);

    const listed = await runCli([
      'acl',
      'list',
      '--resource-type',
      'topic',
      '--resource-name',
      TOPIC,
      '--principal',
      PRINCIPAL,
      '--brokers',
      BROKERS,
      '--json',
    ]);
    expect(listed.code).toBe(0);
    const parsed = JSON.parse(listed.stdout) as {
      resources: { resourceName: string; acls: { principal: string }[] }[];
    };
    expect(parsed.resources[0]?.resourceName).toBe(TOPIC);
    expect(parsed.resources[0]?.acls.some((acl) => acl.principal === PRINCIPAL)).toBe(true);
  });

  it('removes the ACL, then confirms it is gone', async () => {
    const removed = await runCli([
      'acl',
      'remove',
      PRINCIPAL,
      '--resource-type',
      'topic',
      '--resource-name',
      TOPIC,
      '--brokers',
      BROKERS,
      '--yes',
    ]);
    expect(removed.code).toBe(0);

    const listed = await runCli([
      'acl',
      'list',
      '--resource-type',
      'topic',
      '--resource-name',
      TOPIC,
      '--principal',
      PRINCIPAL,
      '--brokers',
      BROKERS,
      '--json',
    ]);
    const parsed = JSON.parse(listed.stdout) as { resources: unknown[] };
    expect(parsed.resources).toEqual([]);
  });
});
