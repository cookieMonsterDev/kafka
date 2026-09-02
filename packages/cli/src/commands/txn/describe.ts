import type { Admin } from '@cookiemonsterdev/kafka-core';
import { parseBrokersFlag } from '../../admin/parse-brokers';
import { CliUsageError } from '../../args/coerce';
import type { CommandSpec } from '../../args/define';
import { EXIT_CODES } from '../../errors/exit-codes';
import { stringifyJsonSafe } from '../../output/json';
import { renderTable } from '../../output/table';
import { mapWithConcurrency } from '../topic/concurrency';

const CONCURRENCY = 8;

interface DescribedTransaction {
  readonly transactionalId: string;
  readonly ok: boolean;
  readonly detail?: string;
  readonly state?: string;
  readonly producerId?: string;
  readonly producerEpoch?: number;
  readonly timeoutMs?: number;
}

/**
 * One `describeTransactions` call per transactional id: the broker's response throws on the
 * first id with a non-zero error code, discarding every other id's result in the same call —
 * exactly the hazard `group describe` already works around for `describeGroups`.
 */
async function describeOne(admin: Admin, transactionalId: string): Promise<DescribedTransaction> {
  const { transactionStates } = await admin.describeTransactions([transactionalId]);
  const found = transactionStates[0];
  if (found === undefined) return { transactionalId, ok: false, detail: 'broker returned no result' };
  return {
    transactionalId,
    ok: true,
    state: found.transactionState,
    producerId: found.producerId.toString(),
    producerEpoch: found.producerEpoch,
    timeoutMs: found.transactionTimeoutMs,
  };
}

function renderHuman(results: readonly DescribedTransaction[]): string {
  const rows = results.map((result) =>
    result.ok
      ? [
          result.transactionalId,
          result.state ?? '',
          result.producerId ?? '',
          String(result.producerEpoch ?? ''),
          String(result.timeoutMs ?? ''),
        ]
      : [result.transactionalId, '(error)', result.detail ?? 'failed', '', ''],
  );
  return renderTable(['TRANSACTIONAL ID', 'STATE', 'PRODUCER ID', 'PRODUCER EPOCH', 'TIMEOUT MS'], rows);
}

export const txnDescribeCommand: CommandSpec = {
  path: ['txn', 'describe'],
  summary: 'Describe one or more transactional ids',
  flags: [{ name: 'brokers', type: 'string', brief: 'comma-separated broker list, e.g. localhost:9092' }],
  positionals: [{ name: 'transactionalIds', variadic: true, brief: 'transactional ids to describe' }],
  examples: ['txn describe orders-producer-1 --brokers localhost:9092'],
  exitCodes: [EXIT_CODES.ok, EXIT_CODES.operationFailed, EXIT_CODES.usage, EXIT_CODES.partialBatch],
  async run({ flags, positionals, runtime, output, config }) {
    if (positionals.length === 0) {
      throw new CliUsageError('txn describe requires at least one transactional id');
    }

    const brokers = parseBrokersFlag(flags.brokers);
    const admin = await runtime.openAdmin({ brokers, env: runtime.env, config });
    try {
      let results: DescribedTransaction[];

      if (positionals.length === 1) {
        results = [await describeOne(admin, positionals[0]!)];
      } else {
        results = await mapWithConcurrency(positionals, CONCURRENCY, async (transactionalId) => {
          try {
            return await describeOne(admin, transactionalId);
          } catch (error) {
            return { transactionalId, ok: false, detail: error instanceof Error ? error.message : String(error) };
          }
        });
      }

      output.write({
        human: () => renderHuman(results),
        json: () => stringifyJsonSafe({ transactionStates: results }),
      });

      const okCount = results.filter((r) => r.ok).length;
      if (okCount === results.length) return EXIT_CODES.ok;
      if (okCount === 0) return EXIT_CODES.operationFailed;
      return EXIT_CODES.partialBatch;
    } finally {
      await admin.disconnect();
    }
  },
};
