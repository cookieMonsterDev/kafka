import type { OffsetSpec } from '@cookiemonsterdev/kafka-core';
import { parseBrokersFlag } from '../../admin/parse-brokers';
import { CliUsageError } from '../../args/coerce';
import type { CommandSpec } from '../../args/define';
import { EXIT_CODES } from '../../errors/exit-codes';
import { OFFSET_SPECS } from '../../output/codes';
import { stringifyJsonSafe } from '../../output/json';
import { renderTable } from '../../output/table';

/**
 * The broker's own `ListOffsets` sentinel timestamps for "latest", "earliest", and KIP-734's
 * "max timestamp" — not something this CLI invents. They resolve through
 * `fetchTopicOffsetsByTimestamp`, like a literal millisecond timestamp.
 */
const TIME_SENTINELS: ReadonlyMap<string, bigint> = new Map([
  ['earliest', OFFSET_SPECS.EARLIEST],
  ['latest', OFFSET_SPECS.LATEST],
  ['max-timestamp', OFFSET_SPECS.MAX_TIMESTAMP],
]);

/**
 * Tiered storage boundaries. They resolve through `fetchTopicOffsets`, which reports `-1` for a
 * partition without one instead of substituting the high watermark.
 */
const TIERED_STORAGE_SPECS: ReadonlyMap<string, OffsetSpec> = new Map<string, OffsetSpec>([
  ['earliest-local', OFFSET_SPECS.EARLIEST_LOCAL],
  ['latest-tiered', OFFSET_SPECS.LATEST_TIERED],
  ['earliest-pending-upload', OFFSET_SPECS.EARLIEST_PENDING_UPLOAD],
]);

const TIME_FLAG_VALUES = `${[...TIME_SENTINELS.keys(), ...TIERED_STORAGE_SPECS.keys()]
  .map((name) => `"${name}"`)
  .join(', ')}, or a millisecond timestamp`;

type TimeQuery = { kind: 'timestamp'; timestamp: bigint } | { kind: 'offset-spec'; offsetSpec: OffsetSpec };

function parseTimeFlag(raw: string): TimeQuery {
  const offsetSpec = TIERED_STORAGE_SPECS.get(raw);
  if (offsetSpec !== undefined) return { kind: 'offset-spec', offsetSpec };
  const sentinel = TIME_SENTINELS.get(raw);
  if (sentinel !== undefined) return { kind: 'timestamp', timestamp: sentinel };
  try {
    return { kind: 'timestamp', timestamp: BigInt(raw) };
  } catch {
    throw new CliUsageError(`--time must be ${TIME_FLAG_VALUES}, got "${raw}"`);
  }
}

export const topicOffsetsCommand: CommandSpec = {
  path: ['topic', 'offsets'],
  summary: 'Show partition offsets for a topic',
  flags: [
    { name: 'brokers', type: 'string', brief: 'comma-separated broker list, e.g. localhost:9092' },
    {
      name: 'time',
      type: 'string',
      brief: `resolve offsets as of ${TIME_FLAG_VALUES}`,
    },
  ],
  positionals: [{ name: 'topic', brief: 'topic name' }],
  examples: [
    'topic offsets orders --brokers localhost:9092',
    'topic offsets orders --time earliest --brokers localhost:9092',
    'topic offsets orders --time 1735689600000 --brokers localhost:9092',
    'topic offsets orders --time latest-tiered --brokers localhost:9092',
  ],
  exitCodes: [EXIT_CODES.ok, EXIT_CODES.operationFailed, EXIT_CODES.usage],
  async run({ flags, positionals, runtime, output, config }) {
    const topic = positionals[0];
    if (topic === undefined) {
      throw new CliUsageError('topic offsets requires a topic name');
    }

    const brokers = parseBrokersFlag(flags.brokers);
    const timeFlag = flags.time as string | undefined;
    const timeQuery = timeFlag !== undefined ? parseTimeFlag(timeFlag) : undefined;

    const admin = await runtime.openAdmin({ brokers, env: runtime.env, config });
    try {
      if (timeQuery?.kind === 'timestamp') {
        const entries = await admin.fetchTopicOffsetsByTimestamp(topic, timeQuery.timestamp);
        output.write({
          human: () =>
            renderTable(
              ['PARTITION', 'OFFSET'],
              entries.map((entry) => [String(entry.partition), entry.offset.toString()]),
            ),
          json: () => stringifyJsonSafe({ partitions: entries }),
        });
      } else {
        const entries =
          timeQuery === undefined
            ? await admin.fetchTopicOffsets(topic)
            : await admin.fetchTopicOffsets(topic, { offsetSpec: timeQuery.offsetSpec });
        output.write({
          human: () =>
            renderTable(
              ['PARTITION', 'OFFSET', 'HIGH', 'LOW'],
              entries.map((entry) => [
                String(entry.partition),
                entry.offset.toString(),
                entry.high.toString(),
                entry.low.toString(),
              ]),
            ),
          json: () => stringifyJsonSafe({ partitions: entries }),
        });
      }
      return EXIT_CODES.ok;
    } finally {
      await admin.disconnect();
    }
  },
};
