import { readStdinToEnd } from '../../admin/read-stdin';
import { parseBrokersFlag } from '../../admin/parse-brokers';
import { CliUsageError } from '../../args/coerce';
import type { CommandSpec } from '../../args/define';
import { EXIT_CODES } from '../../errors/exit-codes';
import { stringifyJsonSafe } from '../../output/json';
import { renderTable } from '../../output/table';
import { resolveScramMechanism } from './enums';

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
      const { results } = await admin.alterUserScramCredentials({
        upsertions: positionals.map((name) => ({ name, mechanism, iterations, password })),
      });

      output.write({
        human: () =>
          renderTable(
            ['USER', 'STATUS'],
            results.map((r) => [
              r.user,
              r.errorCode === 0 ? 'set' : (r.errorMessage ?? `failed (code ${String(r.errorCode)})`),
            ]),
          ),
        json: () => stringifyJsonSafe({ results }),
      });

      const okCount = results.filter((r) => r.errorCode === 0).length;
      if (okCount === results.length) return EXIT_CODES.ok;
      if (okCount === 0) return EXIT_CODES.operationFailed;
      return EXIT_CODES.partialBatch;
    } finally {
      await admin.disconnect();
    }
  },
};
