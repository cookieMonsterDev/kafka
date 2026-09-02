import { describe, expect, it } from 'vitest';
import { runCli } from '../helpers/run-cli';

const BROKERS = process.env.KAFKA_BROKERS ?? 'localhost:9092';

/**
 * Runs against a real broker started by `test/helpers/global-setup.ts`. Deliberately read-only:
 * `cluster update-features`, `cluster elect-leaders`, `cluster reassign execute`,
 * `cluster unregister-broker`, and `cluster raft-voter add`/`remove` all mutate cluster topology
 * or quorum membership on the shared 3-broker KRaft compose every integration suite in this
 * package runs against — running any of them here risks leaving that compose in a state the
 * *other* suites (started in the same `beforeAll`, torn down in the same `afterAll`) don't expect,
 * for a family whose read commands already exercise the same connection and admin-open path.
 * Covering those five against a disposable cluster of their own is a gap this suite discloses
 * rather than silently leaves untested.
 */
describe('cluster read commands against a real broker', () => {
  it('describes the cluster', async () => {
    const result = await runCli(['cluster', 'info', '--brokers', BROKERS, '--json']);
    expect(result.code).toBe(0);
    const parsed = JSON.parse(result.stdout) as { brokers: unknown[]; clusterId: string | null };
    expect(parsed.brokers.length).toBeGreaterThan(0);
    expect(parsed.clusterId).not.toBeNull();
  });

  it('describes supported and finalized features', async () => {
    const result = await runCli(['cluster', 'features', '--brokers', BROKERS, '--json']);
    expect(result.code).toBe(0);
    const parsed = JSON.parse(result.stdout) as { supportedFeatures: unknown[] };
    expect(Array.isArray(parsed.supportedFeatures)).toBe(true);
  });

  it('lists active reassignments (none, on a freshly started broker)', async () => {
    const result = await runCli(['cluster', 'reassign', 'list', '--brokers', BROKERS, '--json']);
    expect(result.code).toBe(0);
    const parsed = JSON.parse(result.stdout) as { topics: unknown[] };
    expect(parsed.topics).toEqual([]);
  });
});
