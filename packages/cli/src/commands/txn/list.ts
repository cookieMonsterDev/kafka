import { parseBrokersFlag } from '../../admin/parse-brokers';
import { CliUsageError } from '../../args/coerce';
import type { CommandSpec } from '../../args/define';
import { EXIT_CODES } from '../../errors/exit-codes';
import { stringifyJsonSafe } from '../../output/json';
import { renderTable } from '../../output/table';

function parseBigIntFlag(raw: string, flagName: string): bigint {
  try {
    return BigInt(raw);
  } catch {
    throw new CliUsageError(`--${flagName} expects an integer, got "${raw}"`);
  }
}

export const txnListCommand: CommandSpec = {
  path: ['txn', 'list'],
  summary: 'List transactions known to the cluster, optionally filtered',
  flags: [
    { name: 'brokers', type: 'string', brief: 'comma-separated broker list, e.g. localhost:9092' },
    {
      name: 'state-filter',
      type: 'string',
      multiple: true,
      brief: 'a transaction state to filter on, e.g. Ongoing (repeatable)',
    },
    {
      name: 'producer-id-filter',
      type: 'string',
      multiple: true,
      brief: 'a producer id to filter on (repeatable)',
    },
    { name: 'duration-filter', type: 'string', brief: 'minimum transaction duration in ms' },
    { name: 'transactional-id-pattern', type: 'string', brief: 'a transactional id pattern to filter on' },
  ],
  examples: ['txn list --brokers localhost:9092', 'txn list --state-filter Ongoing --brokers localhost:9092'],
  exitCodes: [EXIT_CODES.ok, EXIT_CODES.operationFailed, EXIT_CODES.usage],
  async run({ flags, runtime, output, config }) {
    const brokers = parseBrokersFlag(flags.brokers);
    const stateFilters = flags['state-filter'] as string[] | undefined;
    const producerIdFilterFlags = flags['producer-id-filter'] as string[] | undefined;
    const producerIdFilters = producerIdFilterFlags?.map((raw) => parseBigIntFlag(raw, 'producer-id-filter'));
    const durationFilterFlag = flags['duration-filter'] as string | undefined;
    const durationFilter =
      durationFilterFlag !== undefined ? parseBigIntFlag(durationFilterFlag, 'duration-filter') : undefined;
    const transactionalIdPattern = flags['transactional-id-pattern'] as string | undefined;

    const admin = await runtime.openAdmin({ brokers, env: runtime.env, config });
    try {
      const { transactionStates } = await admin.listTransactions({
        stateFilters,
        producerIdFilters,
        durationFilter,
        transactionalIdPattern,
      });

      output.write({
        human: () =>
          transactionStates.length === 0
            ? '(no transactions)'
            : renderTable(
                ['TRANSACTIONAL ID', 'PRODUCER ID', 'STATE'],
                transactionStates.map((t) => [t.transactionalId, t.producerId.toString(), t.transactionState]),
              ),
        json: () => stringifyJsonSafe({ transactionStates }),
      });
      return EXIT_CODES.ok;
    } finally {
      await admin.disconnect();
    }
  },
};
