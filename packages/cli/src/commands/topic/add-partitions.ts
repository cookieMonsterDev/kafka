import type { Admin } from '@cookiemonsterdev/kafka-core';
import { parseBrokersFlag } from '../../admin/parse-brokers';
import { CliUsageError } from '../../args/coerce';
import type { CommandSpec } from '../../args/define';
import { EXIT_CODES, exitForBatchResults } from '../../errors/exit-codes';
import { stringifyJsonSafe } from '../../output/json';
import { renderTable } from '../../output/table';
import { mapWithConcurrency } from '../../concurrency';

const CONCURRENCY = 8;

interface TopicResult {
  readonly topic: string;
  readonly ok: boolean;
  readonly detail?: string;
}

async function addOne(admin: Admin, topic: string, count: number, validateOnly: boolean): Promise<TopicResult> {
  await admin.createPartitions({ topicPartitions: [{ topic, count }], validateOnly });
  return { topic, ok: true };
}

export const topicAddPartitionsCommand: CommandSpec = {
  path: ['topic', 'add-partitions'],
  summary: 'Raise a topic to a new total partition count',
  flags: [
    { name: 'brokers', type: 'string', brief: 'comma-separated broker list, e.g. localhost:9092' },
    { name: 'count', type: 'number', brief: 'the new total number of partitions (not a delta)' },
    { name: 'dry-run', type: 'boolean', brief: 'validate without changing anything' },
  ],
  positionals: [{ name: 'topics', variadic: true, brief: 'topic names to raise to --count partitions' }],
  examples: [
    'topic add-partitions orders --count 6 --brokers localhost:9092',
    'topic add-partitions orders payments --count 6 --dry-run',
  ],
  exitCodes: [EXIT_CODES.ok, EXIT_CODES.operationFailed, EXIT_CODES.usage, EXIT_CODES.partialBatch],
  async run({ flags, positionals, runtime, output, config }) {
    if (positionals.length === 0) {
      throw new CliUsageError('topic add-partitions requires at least one topic name');
    }
    const count = flags.count as number | undefined;
    if (count === undefined) {
      throw new CliUsageError('topic add-partitions requires --count');
    }

    const brokers = parseBrokersFlag(flags.brokers);
    const dryRun = flags['dry-run'] === true;

    const admin = await runtime.openAdmin({ brokers, env: runtime.env, config });
    try {
      let results: TopicResult[];

      if (positionals.length === 1) {
        results = [await addOne(admin, positionals[0]!, count, dryRun)];
      } else {
        results = await mapWithConcurrency(positionals, CONCURRENCY, async (topic) => {
          try {
            return await addOne(admin, topic, count, dryRun);
          } catch (error) {
            return { topic, ok: false, detail: error instanceof Error ? error.message : String(error) };
          }
        });
      }

      output.write({
        human: () =>
          renderTable(
            ['TOPIC', 'STATUS'],
            results.map((r) => [r.topic, r.ok ? (dryRun ? 'validated' : 'ok') : (r.detail ?? 'failed')]),
          ),
        json: () => stringifyJsonSafe({ results }),
      });

      return exitForBatchResults(results, (r) => r.ok);
    } finally {
      await admin.disconnect();
    }
  },
};
