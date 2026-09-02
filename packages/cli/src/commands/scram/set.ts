import type { Admin } from '@cookiemonsterdev/kafka-core';
import { readStdinToEnd } from '../../admin/read-stdin';
import { parseBrokersFlag } from '../../admin/parse-brokers';
import { CliUsageError } from '../../args/coerce';
import type { CommandSpec } from '../../args/define';
import { EXIT_CODES } from '../../errors/exit-codes';
import { stringifyJsonSafe } from '../../output/json';
import { renderTable } from '../../output/table';
import { mapWithConcurrency } from '../topic/concurrency';
import { resolveScramMechanism } from './enums';

const CONCURRENCY = 8;

interface UserResult {
  readonly user: string;
  readonly ok: boolean;
  readonly detail?: string;
}

/**
 * One `alterUserScramCredentials` call per user: the broker's response throws on the first
 * upsertion with a non-zero error code, discarding every other user's result in the same call —
 * the same hazard `config set` already works around for `incrementalAlterConfigs`.
 */
async function setOne(
  admin: Admin,
  user: string,
  mechanism: number,
  iterations: number | undefined,
  password: string,
): Promise<UserResult> {
  await admin.alterUserScramCredentials({ upsertions: [{ name: user, mechanism, iterations, password }] });
  return { user, ok: true };
}

export const scramSetCommand: CommandSpec = {
  path: ['scram', 'set'],
  summary: 'Create or update a SCRAM credential for one or more users',
  flags: [
    { name: 'brokers', type: 'string', brief: 'comma-separated broker list, e.g. localhost:9092' },
    { name: 'mechanism', type: 'string', brief: 'scram-sha-256 or scram-sha-512' },
    { name: 'iterations', type: 'number', brief: 'PBKDF2 iteration count (defaults to 4096, the broker minimum)' },
    {
      name: 'password-stdin',
      type: 'boolean',
      brief: 'read the password from stdin — never accepted as a plain flag',
    },
  ],
  positionals: [{ name: 'users', variadic: true, brief: 'user names to set the credential for' }],
  examples: ['scram set alice --mechanism scram-sha-256 --password-stdin --brokers localhost:9092'],
  exitCodes: [EXIT_CODES.ok, EXIT_CODES.operationFailed, EXIT_CODES.usage, EXIT_CODES.partialBatch],
  async run({ flags, positionals, runtime, output, config }) {
    if (positionals.length === 0) {
      throw new CliUsageError('scram set requires at least one user name');
    }
    const mechanismFlag = flags.mechanism as string | undefined;
    if (mechanismFlag === undefined) {
      throw new CliUsageError('scram set requires --mechanism');
    }
    const mechanism = resolveScramMechanism(mechanismFlag);

    if (flags['password-stdin'] !== true) {
      throw new CliUsageError('scram set requires --password-stdin — a password is never accepted as a plain flag');
    }
    const password = await readStdinToEnd(runtime.stdin);
    if (password === '') {
      throw new CliUsageError('scram set received an empty password on stdin');
    }

    const iterations = flags.iterations as number | undefined;

    const brokers = parseBrokersFlag(flags.brokers);
    const admin = await runtime.openAdmin({ brokers, env: runtime.env, config });
    try {
      let results: UserResult[];

      if (positionals.length === 1) {
        results = [await setOne(admin, positionals[0]!, mechanism, iterations, password)];
      } else {
        results = await mapWithConcurrency(positionals, CONCURRENCY, async (user) => {
          try {
            return await setOne(admin, user, mechanism, iterations, password);
          } catch (error) {
            return { user, ok: false, detail: error instanceof Error ? error.message : String(error) };
          }
        });
      }

      output.write({
        human: () =>
          renderTable(
            ['USER', 'STATUS'],
            results.map((r) => [r.user, r.ok ? 'set' : (r.detail ?? 'failed')]),
          ),
        json: () => stringifyJsonSafe({ results }),
      });

      const okCount = results.filter((r) => r.ok).length;
      if (okCount === results.length) return EXIT_CODES.ok;
      if (okCount === 0) return EXIT_CODES.operationFailed;
      return EXIT_CODES.partialBatch;
    } finally {
      await admin.disconnect();
    }
  },
};
