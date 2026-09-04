import type { Admin } from '@cookiemonsterdev/kafka-core';
import { parseBrokersFlag } from '../../admin/parse-brokers';
import type { CommandSpec } from '../../args/define';
import { EXIT_CODES, exitForBatchResults } from '../../errors/exit-codes';
import { describeCode, formatCode, SCRAM_MECHANISMS } from '../../output/codes';
import { stringifyJsonSafe } from '../../output/json';
import { renderTable } from '../../output/table';
import { mapWithConcurrency } from '../../concurrency';

const CONCURRENCY = 8;

interface DescribedUser {
  readonly user: string;
  readonly ok: boolean;
  readonly detail?: string;
  readonly credentials?: readonly { readonly mechanism: number; readonly iterations: number }[];
}

/**
 * One `describeUserScramCredentials` call per named user: the broker's response throws when any
 * requested user has no credential (`RESOURCE_NOT_FOUND` is carried as a per-user error code, but
 * the parsed response discards every other user's result along with it) — exactly the hazard
 * `group describe` already works around for `describeGroups`. Querying with no user names at all
 * (`users: undefined`, "every user") never hits this, since there is nothing to not-find.
 */
async function describeOne(admin: Admin, user: string): Promise<DescribedUser> {
  const { results } = await admin.describeUserScramCredentials({ users: [user] });
  const found = results[0];
  if (found === undefined) return { user, ok: false, detail: 'broker returned no result' };
  return { user, ok: true, credentials: found.credentialInfos };
}

function renderHuman(results: readonly DescribedUser[]): string {
  const rows = results.flatMap((result) => {
    if (!result.ok) return [[result.user, '-', '-', result.detail ?? 'failed']];
    if (result.credentials === undefined || result.credentials.length === 0) {
      return [[result.user, '(none)', '-', 'ok']];
    }
    return result.credentials.map((credential) => [
      result.user,
      formatCode(describeCode(SCRAM_MECHANISMS, credential.mechanism)),
      String(credential.iterations),
      'ok',
    ]);
  });
  return rows.length === 0 ? '(no users)' : renderTable(['USER', 'MECHANISM', 'ITERATIONS', 'STATUS'], rows);
}

export const scramListCommand: CommandSpec = {
  path: ['scram', 'list'],
  summary: 'List SCRAM credentials for one or more users, or every user',
  flags: [{ name: 'brokers', type: 'string', brief: 'comma-separated broker list, e.g. localhost:9092' }],
  positionals: [{ name: 'users', variadic: true, brief: 'user names to describe (omit for every user)' }],
  examples: ['scram list --brokers localhost:9092', 'scram list alice bob --brokers localhost:9092'],
  exitCodes: [EXIT_CODES.ok, EXIT_CODES.operationFailed, EXIT_CODES.usage, EXIT_CODES.partialBatch],
  async run({ flags, positionals, runtime, output, config }) {
    const brokers = parseBrokersFlag(flags.brokers);
    const admin = await runtime.openAdmin({ brokers, env: runtime.env, config });
    try {
      if (positionals.length === 0) {
        const { results } = await admin.describeUserScramCredentials({ users: undefined });
        const described: DescribedUser[] = results.map((result) => ({
          user: result.user,
          ok: true,
          credentials: result.credentialInfos,
        }));
        output.write({
          human: () => renderHuman(described),
          json: () => stringifyJsonSafe({ results: described }),
        });
        return EXIT_CODES.ok;
      }

      let results: DescribedUser[];
      if (positionals.length === 1) {
        results = [await describeOne(admin, positionals[0]!)];
      } else {
        results = await mapWithConcurrency(positionals, CONCURRENCY, async (user) => {
          try {
            return await describeOne(admin, user);
          } catch (error) {
            return { user, ok: false, detail: error instanceof Error ? error.message : String(error) };
          }
        });
      }

      output.write({
        human: () => renderHuman(results),
        json: () => stringifyJsonSafe({ results }),
      });

      return exitForBatchResults(results, (r) => r.ok);
    } finally {
      await admin.disconnect();
    }
  },
};
