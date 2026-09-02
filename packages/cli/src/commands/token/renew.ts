import { parseBrokersFlag } from '../../admin/parse-brokers';
import { CliUsageError } from '../../args/coerce';
import type { CommandSpec } from '../../args/define';
import { EXIT_CODES } from '../../errors/exit-codes';
import { stringifyJsonSafe } from '../../output/json';
import { resolveHmacFlag } from './hmac';

function parseBigIntFlag(raw: string, flagName: string): bigint {
  try {
    return BigInt(raw);
  } catch {
    throw new CliUsageError(`--${flagName} expects an integer, got "${raw}"`);
  }
}

export const tokenRenewCommand: CommandSpec = {
  path: ['token', 'renew'],
  summary: 'Renew a delegation token, extending its expiry',
  flags: [
    { name: 'brokers', type: 'string', brief: 'comma-separated broker list, e.g. localhost:9092' },
    { name: 'hmac', type: 'string', brief: "the token's hmac, base64 (mutually exclusive with --hmac-stdin)" },
    { name: 'hmac-stdin', type: 'boolean', brief: "read the token's hmac (base64) from stdin" },
    { name: 'renew-time-period-ms', type: 'string', brief: 'how long to extend the expiry by, in ms' },
  ],
  examples: ['token renew --hmac-stdin --renew-time-period-ms 86400000 --brokers localhost:9092'],
  exitCodes: [EXIT_CODES.ok, EXIT_CODES.operationFailed, EXIT_CODES.usage],
  async run({ flags, runtime, output, config }) {
    const hmac = await resolveHmacFlag({
      hmacFlag: flags.hmac as string | undefined,
      hmacStdin: flags['hmac-stdin'] === true,
      stdin: runtime.stdin,
    });

    const renewTimePeriodMsFlag = flags['renew-time-period-ms'] as string | undefined;
    const renewTimePeriodMs =
      renewTimePeriodMsFlag !== undefined ? parseBigIntFlag(renewTimePeriodMsFlag, 'renew-time-period-ms') : undefined;

    const brokers = parseBrokersFlag(flags.brokers);
    const admin = await runtime.openAdmin({ brokers, env: runtime.env, config });
    try {
      const { expiryTimestamp } = await admin.renewDelegationToken({ hmac, renewTimePeriodMs });
      output.write({
        human: () => `renewed, new expiry: ${expiryTimestamp.toString()}`,
        json: () => stringifyJsonSafe({ expiryTimestamp }),
      });
      return EXIT_CODES.ok;
    } finally {
      await admin.disconnect();
    }
  },
};
