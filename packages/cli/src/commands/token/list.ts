import { parseBrokersFlag } from '../../admin/parse-brokers';
import { redactSecrets } from '../../admin/redact';
import type { CommandSpec } from '../../args/define';
import { EXIT_CODES } from '../../errors/exit-codes';
import { stringifyJsonSafe } from '../../output/json';
import { renderTable } from '../../output/table';
import { parsePrincipalFlags } from './principal';

export const tokenListCommand: CommandSpec = {
  path: ['token', 'list'],
  summary: 'List delegation tokens, optionally filtered by owner',
  flags: [
    { name: 'brokers', type: 'string', brief: 'comma-separated broker list, e.g. localhost:9092' },
    {
      name: 'owner',
      type: 'string',
      multiple: true,
      brief: 'an owning principal to filter on, "PrincipalType:name" (repeatable)',
    },
    { name: 'show-secrets', type: 'boolean', brief: 'print each token hmac instead of redacting it' },
  ],
  examples: ['token list --brokers localhost:9092', 'token list --owner User:alice --brokers localhost:9092'],
  exitCodes: [EXIT_CODES.ok, EXIT_CODES.operationFailed, EXIT_CODES.usage],
  async run({ flags, runtime, output, config }) {
    const owners = parsePrincipalFlags(flags.owner as string[] | undefined, 'owner');
    const brokers = parseBrokersFlag(flags.brokers);

    const admin = await runtime.openAdmin({ brokers, env: runtime.env, config });
    try {
      const { tokens } = await admin.describeDelegationToken({ owners });
      const showSecrets = flags['show-secrets'] === true;

      output.write({
        human: () =>
          tokens.length === 0
            ? '(no tokens)'
            : renderTable(
                showSecrets ? ['TOKEN ID', 'OWNER', 'EXPIRY', 'HMAC'] : ['TOKEN ID', 'OWNER', 'EXPIRY'],
                tokens.map((token) => [
                  token.tokenId,
                  `${token.owner.principalType}:${token.owner.name}`,
                  token.expiryTimestamp.toString(),
                  ...(showSecrets ? [token.hmac.toString('base64')] : []),
                ]),
              ),
        json: () => stringifyJsonSafe({ tokens: showSecrets ? tokens : redactSecrets(tokens) }),
      });
      return EXIT_CODES.ok;
    } finally {
      await admin.disconnect();
    }
  },
};
