import { parseBrokersFlag } from '../../admin/parse-brokers';
import { CliUsageError } from '../../args/coerce';
import type { CommandSpec } from '../../args/define';
import { EXIT_CODES } from '../../errors/exit-codes';
import { stringifyJsonSafe } from '../../output/json';
import { renderTable } from '../../output/table';

/**
 * `-1`/`-2`/`-3` are the broker's own `ListOffsets` sentinel timestamps for "latest", "earliest",
 * and KIP-568's "max timestamp" — not something this CLI invents.
 */
const TIME_SENTINELS: Readonly<Record<string, bigint>> = {
  earliest: -2n,
  latest: -1n,
  'max-timestamp': -3n,
};

function parseTimeFlag(raw: string): bigint {
  const sentinel = TIME_SENTINELS[raw];
  if (sentinel !== undefined) return sentinel;
  try {
    return BigInt(raw);
  } catch {
    throw new CliUsageError(
      `--time must be "earliest", "latest", "max-timestamp", or a millisecond timestamp, got "${raw}"`,
    );
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
      brief: 'resolve offsets as of "earliest", "latest", "max-timestamp", or a millisecond timestamp',
    },
  ],
  positionals: [{ name: 'topic', brief: 'topic name' }],
  examples: [
    'topic offsets orders --brokers localhost:9092',
    'topic offsets orders --time earliest --brokers localhost:9092',
    'topic offsets orders --time 1735689600000 --brokers localhost:9092',
  ],
  exitCodes: [EXIT_CODES.ok, EXIT_CODES.operationFailed, EXIT_CODES.usage],
  async run({ flags, positionals, runtime, output, config }) {
    const topic = positionals[0];
    if (topic === undefined) {
      throw new CliUsageError('topic offsets requires a topic name');
    }

    const brokers = parseBrokersFlag(flags.brokers);
    const timeFlag = flags.time as string | undefined;
    const timestamp = timeFlag !== undefined ? parseTimeFlag(timeFlag) : undefined;

    const admin = await runtime.openAdmin({ brokers, env: runtime.env, config });
    try {
      if (timestamp !== undefined) {
        const entries = await admin.fetchTopicOffsetsByTimestamp(topic, timestamp);
        output.write({
          human: () =>
            renderTable(
              ['PARTITION', 'OFFSET'],
              entries.map((entry) => [String(entry.partition), entry.offset.toString()]),
            ),
          json: () => stringifyJsonSafe({ partitions: entries }),
        });
      } else {
        const entries = await admin.fetchTopicOffsets(topic);
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
