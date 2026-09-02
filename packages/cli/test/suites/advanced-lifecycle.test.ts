import { describe, expect, it } from 'vitest';
import { runCli } from '../helpers/run-cli';

const BROKERS = process.env.KAFKA_BROKERS ?? 'localhost:9092';
const UNIQUE = `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;

/**
 * This suite's compose matrix (`test/helpers/global-setup.ts`) only ever selects `0.10`, `4.0`,
 * or `4.3` — share groups (KIP-932) need a broker at least at 4.1, which among those three is
 * only `4.3`.
 */
const SUPPORTS_SHARE_GROUPS = (process.env.KAFKA_VERSION ?? '4.0') === '4.3';

/**
 * Runs against a real broker started by `test/helpers/global-setup.ts`. Transactions, SCRAM
 * credentials, quotas, and share group topology are all cluster-wide state shared with every
 * other suite in this package's `beforeAll`/`afterAll` — like `cluster-lifecycle.test.ts`, this
 * deliberately stays read-only (or, for `quota alter`, validate-only) rather than risk leaving
 * that shared cluster in a state another suite doesn't expect. `scram set`'s stdin-only password
 * round trip is exercised at the unit level instead (`src/commands/scram/set.test.ts`), where a
 * fake stdin can be driven deterministically — this suite's harness has no interactive stdin to
 * plug a password into. Delegation tokens are skipped entirely: the broker rejects every
 * `Describe/Create/Renew/ExpireDelegationToken` request outright over a PLAINTEXT listener
 * ("Delegation Token requests are not allowed on PLAINTEXT/1-way SSL channels..."), and this
 * package's `openAdmin` has no SASL support yet (the same pre-existing gap `group-lifecycle.test.ts`
 * already discloses) — `token create/renew/expire/list` are exercised at the unit level instead
 * (`src/commands/token/*.test.ts`). Covering every write path against a disposable cluster of its
 * own is a gap this suite discloses rather than silently leaves untested.
 */
describe('transaction, scram, quota, and share group read commands against a real broker', () => {
  it('lists transactions (empty or not) without error', async () => {
    const result = await runCli(['txn', 'list', '--brokers', BROKERS, '--json']);
    expect(result.code).toBe(0);
    const parsed = JSON.parse(result.stdout) as { transactionStates: unknown[] };
    expect(Array.isArray(parsed.transactionStates)).toBe(true);
  });

  it('exits non-zero describing a transactional id that has never existed', async () => {
    const result = await runCli(['txn', 'describe', `kafka-cli-it-nonexistent-txn-${UNIQUE}`, '--brokers', BROKERS]);
    expect(result.code).not.toBe(0);
  });

  it('lists every SCRAM user (empty or not) without error', async () => {
    const result = await runCli(['scram', 'list', '--brokers', BROKERS, '--json']);
    expect(result.code).toBe(0);
    const parsed = JSON.parse(result.stdout) as { results: unknown[] };
    expect(Array.isArray(parsed.results)).toBe(true);
  });

  it('exits non-zero describing a SCRAM user that has never existed', async () => {
    const result = await runCli(['scram', 'list', `kafka-cli-it-nonexistent-user-${UNIQUE}`, '--brokers', BROKERS]);
    expect(result.code).not.toBe(0);
  });

  it('describes client quotas matching a filter that matches nothing', async () => {
    const result = await runCli([
      'quota',
      'describe',
      '--entity',
      `user=kafka-cli-it-nonexistent-user-${UNIQUE}`,
      '--brokers',
      BROKERS,
      '--json',
    ]);
    expect(result.code).toBe(0);
    const parsed = JSON.parse(result.stdout) as { entries: unknown[] };
    expect(parsed.entries).toEqual([]);
  });

  it('validates a quota alteration without changing anything', async () => {
    const result = await runCli([
      'quota',
      'alter',
      '--entity',
      `user=kafka-cli-it-quota-${UNIQUE}`,
      '--set',
      'producer_byte_rate=1048576',
      '--dry-run',
      '--brokers',
      BROKERS,
      '--json',
    ]);
    expect(result.code).toBe(0);
  });

  it.runIf(SUPPORTS_SHARE_GROUPS)('lists share groups (empty or not) without error', async () => {
    const result = await runCli(['share-group', 'list', '--brokers', BROKERS, '--json']);
    expect(result.code).toBe(0);
    const parsed = JSON.parse(result.stdout) as { groups: unknown[] };
    expect(Array.isArray(parsed.groups)).toBe(true);
  });

  it.runIf(SUPPORTS_SHARE_GROUPS)('exits non-zero deleting a share group that has never existed', async () => {
    const result = await runCli([
      'share-group',
      'delete',
      `kafka-cli-it-nonexistent-share-group-${UNIQUE}`,
      '--yes',
      '--brokers',
      BROKERS,
    ]);
    expect(result.code).not.toBe(0);
  });
});
