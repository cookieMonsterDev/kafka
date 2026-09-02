import { parseBrokersFlag } from '../../admin/parse-brokers';
import { CliUsageError } from '../../args/coerce';
import type { CommandSpec } from '../../args/define';
import { EXIT_CODES } from '../../errors/exit-codes';
import { confirmDestructive } from '../../interaction/confirm';
import { stringifyJsonSafe } from '../../output/json';
import { resolveHmacFlag } from './hmac';

/** `kafka-delegation-tokens.sh --expiry-time-period -1`'s sentinel for "expire immediately". */
const EXPIRE_IMMEDIATELY = -1n;

function parseBigIntFlag(raw: string, flagName: string): bigint {
  try {
    return BigInt(raw);
  } catch {
    throw new CliUsageError(`--${flagName} expects an integer, got "${raw}"`);
  }
}

export const tokenExpireCommand: CommandSpec = {
  path: ['token', 'expire'],
  summary: 'Expire a delegation token, immediately by default',
  flags: [
    { name: 'brokers', type: 'string', brief: 'comma-separated broker list, e.g. localhost:9092' },
    { name: 'hmac', type: 'string', brief: "the token's hmac, base64 (mutually exclusive with --hmac-stdin)" },
    { name: 'hmac-stdin', type: 'boolean', brief: "read the token's hmac (base64) from stdin" },
    { name: 'expiry-time-period-ms', type: 'string', brief: 'ms until expiry (defaults to immediate expiry)' },
    { name: 'yes', type: 'boolean', brief: 'confirm the expiry without an interactive prompt' },
  ],
  examples: ['token expire --hmac-stdin --brokers localhost:9092 --yes'],
  exitCodes: [EXIT_CODES.ok, EXIT_CODES.operationFailed, EXIT_CODES.usage, EXIT_CODES.abortedOrUnconfirmed],
  async run({ flags, runtime, output, config }) {
    const hmac = await resolveHmacFlag({
      hmacFlag: flags.hmac as string | undefined,
      hmacStdin: flags['hmac-stdin'] === true,
      stdin: runtime.stdin,
    });

    const expiryTimePeriodMsFlag = flags['expiry-time-period-ms'] as string | undefined;
    const expiryTimePeriodMs =
      expiryTimePeriodMsFlag !== undefined
        ? parseBigIntFlag(expiryTimePeriodMsFlag, 'expiry-time-period-ms')
        : EXPIRE_IMMEDIATELY;

    await confirmDestructive({
      runtime,
      yes: flags.yes === true,
      message: 'Expire this delegation token?',
      confirmDestructive: config.cli.confirmDestructive,
    });

    const brokers = parseBrokersFlag(flags.brokers);
    const admin = await runtime.openAdmin({ brokers, env: runtime.env, config });
    try {
      const { expiryTimestamp } = await admin.expireDelegationToken({ hmac, expiryTimePeriodMs });
      output.write({
        human: () => `expired, expiry set to: ${expiryTimestamp.toString()}`,
        json: () => stringifyJsonSafe({ expiryTimestamp }),
      });
      return EXIT_CODES.ok;
    } finally {
      await admin.disconnect();
    }
  },
};
