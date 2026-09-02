import { parseBrokersFlag } from '../../admin/parse-brokers';
import { CliUsageError } from '../../args/coerce';
import type { CommandSpec } from '../../args/define';
import { EXIT_CODES } from '../../errors/exit-codes';
import { confirmDestructive } from '../../interaction/confirm';
import { stringifyJsonSafe } from '../../output/json';

export const txnTerminateCommand: CommandSpec = {
  path: ['txn', 'terminate'],
  summary: "Force-terminate a transactional id's current transaction",
  flags: [
    { name: 'brokers', type: 'string', brief: 'comma-separated broker list, e.g. localhost:9092' },
    { name: 'timeout', type: 'number', brief: 'transaction timeout in ms for the fenced producer' },
    { name: 'yes', type: 'boolean', brief: 'confirm the termination without an interactive prompt' },
  ],
  positionals: [{ name: 'transactionalId', brief: 'transactional id to terminate' }],
  examples: ['txn terminate orders-producer-1 --brokers localhost:9092 --yes'],
  exitCodes: [EXIT_CODES.ok, EXIT_CODES.operationFailed, EXIT_CODES.usage, EXIT_CODES.abortedOrUnconfirmed],
  async run({ flags, positionals, runtime, output, config }) {
    const transactionalId = positionals[0];
    if (transactionalId === undefined) {
      throw new CliUsageError('txn terminate requires a transactional id');
    }

    const transactionTimeout = flags.timeout as number | undefined;

    await confirmDestructive({
      runtime,
      yes: flags.yes === true,
      message: `Force-terminate the current transaction for "${transactionalId}"?`,
      confirmDestructive: config.cli.confirmDestructive,
    });

    const brokers = parseBrokersFlag(flags.brokers);
    const admin = await runtime.openAdmin({ brokers, env: runtime.env, config });
    try {
      const result = await admin.forceTerminateTransaction({ transactionalId, transactionTimeout });

      output.write({
        human: () =>
          result.errorCode === 0
            ? `terminated (producer ${result.producerId?.toString() ?? '-'}, epoch ${result.producerEpoch !== undefined ? String(result.producerEpoch) : '-'})`
            : `failed (code ${String(result.errorCode)})`,
        json: () => stringifyJsonSafe({ result }),
      });

      return result.errorCode === 0 ? EXIT_CODES.ok : EXIT_CODES.operationFailed;
    } finally {
      await admin.disconnect();
    }
  },
};
