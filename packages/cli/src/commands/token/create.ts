import { parseBrokersFlag } from '../../admin/parse-brokers';
import { redactSecrets } from '../../admin/redact';
import { coerceBigInt } from '../../args/coerce';
import type { CommandSpec } from '../../args/define';
import { EXIT_CODES } from '../../errors/exit-codes';
import { bigintAwareReplacer } from '../../output/bigint-replacer';
import { stringifyJsonSafe } from '../../output/json';
import { parsePrincipalFlag, parsePrincipalFlags } from './principal';

export const tokenCreateCommand: CommandSpec = {
  path: ['token', 'create'],
  summary: 'Create a delegation token',
  flags: [
    { name: 'brokers', type: 'string', brief: 'comma-separated broker list, e.g. localhost:9092' },
    {
      name: 'owner',
      type: 'string',
      brief: 'owning principal, "PrincipalType:name" (defaults to the authenticated user)',
    },
    {
      name: 'renewer',
      type: 'string',
      multiple: true,
      brief: 'a principal allowed to renew the token, "PrincipalType:name" (repeatable)',
    },
    {
      name: 'max-life-time-ms',
      type: 'string',
      brief: 'maximum token lifetime in ms (defaults to the broker setting)',
    },
    { name: 'show-secrets', type: 'boolean', brief: 'print the token hmac instead of redacting it' },
  ],
  examples: ['token create --renewer User:alice --brokers localhost:9092 --show-secrets'],
  exitCodes: [EXIT_CODES.ok, EXIT_CODES.operationFailed, EXIT_CODES.usage],
  async run({ flags, runtime, output, config }) {
    const ownerFlag = flags.owner as string | undefined;
    const owner = ownerFlag !== undefined ? parsePrincipalFlag(ownerFlag, 'owner') : undefined;
    const renewers = parsePrincipalFlags(flags.renewer as string[] | undefined, 'renewer');
    const maxLifeTimeMsFlag = flags['max-life-time-ms'] as string | undefined;
    const maxLifeTimeMs =
      maxLifeTimeMsFlag !== undefined ? coerceBigInt(maxLifeTimeMsFlag, 'max-life-time-ms') : undefined;

    const brokers = parseBrokersFlag(flags.brokers);
    const admin = await runtime.openAdmin({ brokers, env: runtime.env, config });
    try {
      const result = await admin.createDelegationToken({ owner, renewers, maxLifeTimeMs });
      const shown = flags['show-secrets'] === true ? result : redactSecrets(result);

      output.write({
        human: () => JSON.stringify(shown, bigintAwareReplacer, 2),
        json: () => stringifyJsonSafe(shown),
      });
      return EXIT_CODES.ok;
    } finally {
      await admin.disconnect();
    }
  },
};
