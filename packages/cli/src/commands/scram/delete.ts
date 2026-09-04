import type { Admin } from '@cookiemonsterdev/kafka-core';
import { parseBrokersFlag } from '../../admin/parse-brokers';
import { CliUsageError } from '../../args/coerce';
import type { CommandSpec } from '../../args/define';
import { EXIT_CODES, exitForBatchResults } from '../../errors/exit-codes';
import { confirmDestructive } from '../../interaction/confirm';
import { stringifyJsonSafe } from '../../output/json';
import { renderTable } from '../../output/table';
import { mapWithConcurrency } from '../../concurrency';
import { resolveScramMechanism } from './enums';

const CONCURRENCY = 8;

interface UserResult {
  readonly user: string;
  readonly ok: boolean;
  readonly detail?: string;
}

/**
 * One `alterUserScramCredentials` call per user: the broker's response throws on the first
 * deletion with a non-zero error code (a user with no credential to delete included), discarding
 * every other user's result in the same call — the same hazard `scram set` works around.
 */
async function deleteOne(admin: Admin, user: string, mechanism: number): Promise<UserResult> {
  await admin.alterUserScramCredentials({ deletions: [{ name: user, mechanism }] });
  return { user, ok: true };
}

export const scramDeleteCommand: CommandSpec = {
  path: ['scram', 'delete'],
  summary: 'Delete a SCRAM credential for one or more users',
  flags: [
    { name: 'brokers', type: 'string', brief: 'comma-separated broker list, e.g. localhost:9092' },
    { name: 'mechanism', type: 'string', brief: 'scram-sha-256 or scram-sha-512' },
    { name: 'yes', type: 'boolean', brief: 'confirm the deletion without an interactive prompt' },
  ],
  positionals: [{ name: 'users', variadic: true, brief: 'user names to delete the credential for' }],
  examples: ['scram delete alice --mechanism scram-sha-256 --brokers localhost:9092 --yes'],
  exitCodes: [
    EXIT_CODES.ok,
    EXIT_CODES.operationFailed,
    EXIT_CODES.usage,
    EXIT_CODES.partialBatch,
    EXIT_CODES.abortedOrUnconfirmed,
  ],
  async run({ flags, positionals, runtime, output, config }) {
    if (positionals.length === 0) {
      throw new CliUsageError('scram delete requires at least one user name');
    }
    const mechanismFlag = flags.mechanism as string | undefined;
    if (mechanismFlag === undefined) {
      throw new CliUsageError('scram delete requires --mechanism');
    }
    const mechanism = resolveScramMechanism(mechanismFlag);

    await confirmDestructive({
      runtime,
      yes: flags.yes === true,
      message: `Delete the ${mechanismFlag} credential for ${positionals.join(', ')}?`,
      confirmDestructive: config.cli.confirmDestructive,
    });

    const brokers = parseBrokersFlag(flags.brokers);
    const admin = await runtime.openAdmin({ brokers, env: runtime.env, config });
    try {
      let results: UserResult[];

      if (positionals.length === 1) {
        results = [await deleteOne(admin, positionals[0]!, mechanism)];
      } else {
        results = await mapWithConcurrency(positionals, CONCURRENCY, async (user) => {
          try {
            return await deleteOne(admin, user, mechanism);
          } catch (error) {
            return { user, ok: false, detail: error instanceof Error ? error.message : String(error) };
          }
        });
      }

      output.write({
        human: () =>
          renderTable(
            ['USER', 'STATUS'],
            results.map((r) => [r.user, r.ok ? 'deleted' : (r.detail ?? 'failed')]),
          ),
        json: () => stringifyJsonSafe({ results }),
      });

      return exitForBatchResults(results, (r) => r.ok);
    } finally {
      await admin.disconnect();
    }
  },
};
