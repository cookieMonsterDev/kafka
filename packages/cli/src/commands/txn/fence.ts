import { parseBrokersFlag } from '../../admin/parse-brokers';
import { CliUsageError } from '../../args/coerce';
import type { CommandSpec } from '../../args/define';
import { EXIT_CODES } from '../../errors/exit-codes';
import { stringifyJsonSafe } from '../../output/json';
import { renderTable } from '../../output/table';

export const txnFenceCommand: CommandSpec = {
  path: ['txn', 'fence'],
  summary: "Fence out a transactional id's current producer, bumping its epoch",
  flags: [
    { name: 'brokers', type: 'string', brief: 'comma-separated broker list, e.g. localhost:9092' },
    { name: 'timeout', type: 'number', brief: 'transaction timeout in ms for the fenced producer (default 60000)' },
  ],
  positionals: [{ name: 'transactionalIds', variadic: true, brief: 'transactional ids to fence' }],
  examples: ['txn fence orders-producer-1 --brokers localhost:9092'],
  exitCodes: [EXIT_CODES.ok, EXIT_CODES.operationFailed, EXIT_CODES.usage, EXIT_CODES.partialBatch],
  async run({ flags, positionals, runtime, output, config }) {
    if (positionals.length === 0) {
      throw new CliUsageError('txn fence requires at least one transactional id');
    }

    const transactionTimeout = flags.timeout as number | undefined;

    const brokers = parseBrokersFlag(flags.brokers);
    const admin = await runtime.openAdmin({ brokers, env: runtime.env, config });
    try {
      const { results } = await admin.fenceProducers({ transactionalIds: [...positionals], transactionTimeout });

      output.write({
        human: () =>
          renderTable(
            ['TRANSACTIONAL ID', 'STATUS', 'PRODUCER ID', 'PRODUCER EPOCH'],
            results.map((r) => [
              r.transactionalId,
              r.errorCode === 0 ? 'fenced' : `failed (code ${String(r.errorCode)})`,
              r.producerId?.toString() ?? '-',
              r.producerEpoch !== undefined ? String(r.producerEpoch) : '-',
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
