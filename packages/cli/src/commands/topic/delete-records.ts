import type { Admin } from '@cookiemonsterdev/kafka-core';
import { parseBrokersFlag } from '../../admin/parse-brokers';
import { readJsonFile } from '../../admin/read-json-file';
import { CliUsageError } from '../../args/coerce';
import type { CommandSpec } from '../../args/define';
import { EXIT_CODES } from '../../errors/exit-codes';
import { confirmDestructive } from '../../interaction/confirm';
import { stringifyJsonSafe } from '../../output/json';
import { renderTable } from '../../output/table';
import { mapWithConcurrency } from './concurrency';

const CONCURRENCY = 8;
const FROM_FILE_FLAG = 'from-file';

// Not exported from `@cookiemonsterdev/kafka-core`'s public surface — mirrors `Admin.deleteTopicRecords`'s own inline parameter shape.
interface SeekInput {
  readonly partition: number;
  readonly offset: number | string | bigint;
}

interface TopicResult {
  readonly topic: string;
  readonly ok: boolean;
  readonly detail?: string;
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

/**
 * Validates and normalizes the `--offset-json-file`-shaped input `kafka-delete-records.sh` uses
 * (`{"partitions": [{"topic", "partition", "offset"}, ...], "version": 1}`), then groups its
 * entries by topic — core's `deleteTopicRecords` is one call per topic, unlike the cross-topic
 * file format.
 */
function parseDeleteRecordsFile(raw: unknown): ReadonlyMap<string, SeekInput[]> {
  if (!isPlainObject(raw) || !Array.isArray(raw.partitions)) {
    throw new CliUsageError(`--${FROM_FILE_FLAG} must be a JSON object with a "partitions" array`);
  }

  const byTopic = new Map<string, SeekInput[]>();
  raw.partitions.forEach((entry: unknown, index: number) => {
    if (
      !isPlainObject(entry) ||
      typeof entry.topic !== 'string' ||
      typeof entry.partition !== 'number' ||
      (typeof entry.offset !== 'number' && typeof entry.offset !== 'string')
    ) {
      throw new CliUsageError(
        `--${FROM_FILE_FLAG} "partitions[${index}]" must have a string "topic", a numeric "partition", and a numeric or string "offset"`,
      );
    }
    const partitions = byTopic.get(entry.topic) ?? [];
    partitions.push({ partition: entry.partition, offset: entry.offset });
    byTopic.set(entry.topic, partitions);
  });

  if (byTopic.size === 0) {
    throw new CliUsageError(`--${FROM_FILE_FLAG} lists no partitions`);
  }
  return byTopic;
}

async function deleteRecordsForTopic(admin: Admin, topic: string, partitions: SeekInput[]): Promise<TopicResult> {
  await admin.deleteTopicRecords({ topic, partitions });
  return { topic, ok: true };
}

export const topicDeleteRecordsCommand: CommandSpec = {
  path: ['topic', 'delete-records'],
  summary: 'Delete records before a given offset, per partition',
  flags: [
    { name: 'brokers', type: 'string', brief: 'comma-separated broker list, e.g. localhost:9092' },
    {
      name: FROM_FILE_FLAG,
      type: 'string',
      brief: 'path to a JSON file in the kafka-delete-records.sh --offset-json-file shape',
    },
    { name: 'yes', type: 'boolean', brief: 'confirm the deletion without an interactive prompt' },
  ],
  examples: ['topic delete-records --from-file offsets.json --brokers localhost:9092 --yes'],
  exitCodes: [
    EXIT_CODES.ok,
    EXIT_CODES.operationFailed,
    EXIT_CODES.usage,
    EXIT_CODES.partialBatch,
    EXIT_CODES.abortedOrUnconfirmed,
  ],
  async run({ flags, runtime, output, config }) {
    if (typeof flags[FROM_FILE_FLAG] !== 'string') {
      throw new CliUsageError(`topic delete-records requires --${FROM_FILE_FLAG}`);
    }
    const byTopic = parseDeleteRecordsFile(readJsonFile(flags[FROM_FILE_FLAG], FROM_FILE_FLAG));
    const topics = [...byTopic.keys()];

    const brokers = parseBrokersFlag(flags.brokers);
    const yes = flags.yes === true;

    await confirmDestructive({
      runtime,
      yes,
      message: `Delete records from topic${topics.length > 1 ? 's' : ''} ${topics.join(', ')}?`,
      confirmDestructive: config.cli.confirmDestructive,
    });

    const admin = await runtime.openAdmin({ brokers, env: runtime.env, config });
    try {
      let results: TopicResult[];

      if (topics.length === 1) {
        const topic = topics[0]!;
        results = [await deleteRecordsForTopic(admin, topic, byTopic.get(topic)!)];
      } else {
        results = await mapWithConcurrency(topics, CONCURRENCY, async (topic) => {
          try {
            return await deleteRecordsForTopic(admin, topic, byTopic.get(topic)!);
          } catch (error) {
            return { topic, ok: false, detail: error instanceof Error ? error.message : String(error) };
          }
        });
      }

      output.write({
        human: () =>
          renderTable(
            ['TOPIC', 'STATUS'],
            results.map((r) => [r.topic, r.ok ? 'ok' : (r.detail ?? 'failed')]),
          ),
        json: () => stringifyJsonSafe({ results }),
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
