import { parseBrokersFlag } from '../../admin/parse-brokers';
import { CliUsageError } from '../../args/coerce';
import type { CommandSpec } from '../../args/define';
import { EXIT_CODES } from '../../errors/exit-codes';
import { stringifyJsonSafe } from '../../output/json';
import { renderTable } from '../../output/table';

export const txnDescribeCommand: CommandSpec = {
  path: ['txn', 'describe'],
  summary: 'Describe one or more transactional ids',
  flags: [{ name: 'brokers', type: 'string', brief: 'comma-separated broker list, e.g. localhost:9092' }],
  positionals: [{ name: 'transactionalIds', variadic: true, brief: 'transactional ids to describe' }],
  examples: ['txn describe orders-producer-1 --brokers localhost:9092'],
  exitCodes: [EXIT_CODES.ok, EXIT_CODES.operationFailed, EXIT_CODES.usage],
  async run({ flags, positionals, runtime, output, config }) {
    if (positionals.length === 0) {
      throw new CliUsageError('txn describe requires at least one transactional id');
    }

    const brokers = parseBrokersFlag(flags.brokers);
    const admin = await runtime.openAdmin({ brokers, env: runtime.env, config });
    try {
      const { transactionStates } = await admin.describeTransactions([...positionals]);

      output.write({
        human: () =>
          renderTable(
            ['TRANSACTIONAL ID', 'STATE', 'PRODUCER ID', 'PRODUCER EPOCH', 'TIMEOUT MS'],
            transactionStates.map((t) => [
              t.transactionalId,
              t.errorCode === 0 ? t.transactionState : `error (code ${String(t.errorCode)})`,
              t.producerId.toString(),
              String(t.producerEpoch),
              String(t.transactionTimeoutMs),
            ]),
          ),
        json: () => stringifyJsonSafe({ transactionStates }),
      });
      return EXIT_CODES.ok;
    } finally {
      await admin.disconnect();
    }
  },
};
