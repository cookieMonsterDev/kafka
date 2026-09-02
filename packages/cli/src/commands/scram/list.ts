import { parseBrokersFlag } from '../../admin/parse-brokers';
import type { CommandSpec } from '../../args/define';
import { EXIT_CODES } from '../../errors/exit-codes';
import { describeCode, formatCode, SCRAM_MECHANISMS } from '../../output/codes';
import { stringifyJsonSafe } from '../../output/json';
import { renderTable } from '../../output/table';

export const scramListCommand: CommandSpec = {
  path: ['scram', 'list'],
  summary: 'List SCRAM credentials for one or more users, or every user',
  flags: [{ name: 'brokers', type: 'string', brief: 'comma-separated broker list, e.g. localhost:9092' }],
  positionals: [{ name: 'users', variadic: true, brief: 'user names to describe (omit for every user)' }],
  examples: ['scram list --brokers localhost:9092', 'scram list alice bob --brokers localhost:9092'],
  exitCodes: [EXIT_CODES.ok, EXIT_CODES.operationFailed, EXIT_CODES.usage],
  async run({ flags, positionals, runtime, output, config }) {
    const brokers = parseBrokersFlag(flags.brokers);
    const users = positionals.length > 0 ? [...positionals] : undefined;

    const admin = await runtime.openAdmin({ brokers, env: runtime.env, config });
    try {
      const { results } = await admin.describeUserScramCredentials({ users });

      const rows = results.flatMap((result) => {
        if (result.errorCode !== 0) {
          return [[result.user, '-', '-', result.errorMessage ?? `failed (code ${String(result.errorCode)})`]];
        }
        if (result.credentialInfos.length === 0) {
          return [[result.user, '(none)', '-', 'ok']];
        }
        return result.credentialInfos.map((credential) => [
          result.user,
          formatCode(describeCode(SCRAM_MECHANISMS, credential.mechanism)),
          String(credential.iterations),
          'ok',
        ]);
      });

      output.write({
        human: () =>
          rows.length === 0 ? '(no users)' : renderTable(['USER', 'MECHANISM', 'ITERATIONS', 'STATUS'], rows),
        json: () => stringifyJsonSafe({ results }),
      });
      return EXIT_CODES.ok;
    } finally {
      await admin.disconnect();
    }
  },
};
