import { parseBrokersFlag } from '../../admin/parse-brokers';
import { CliUsageError, coerceNumber } from '../../args/coerce';
import type { CommandSpec } from '../../args/define';
import { EXIT_CODES } from '../../errors/exit-codes';
import { confirmDestructive } from '../../interaction/confirm';
import { stringifyJsonSafe } from '../../output/json';
import { renderTable } from '../../output/table';

interface SetEntry {
  readonly topic: string;
  readonly partition: number;
  readonly offset: bigint;
}

function parseSetFlags(raw: readonly string[]): SetEntry[] {
  return raw.map((entry) => {
    const parts = entry.split(':');
    if (parts.length !== 3) {
      throw new CliUsageError(`--set must be "topic:partition:offset", got "${entry}"`);
    }
    const [topic, partitionRaw, offsetRaw] = parts as [string, string, string];
    const partition = coerceNumber(partitionRaw, 'set');
    let offset: bigint;
    try {
      offset = BigInt(offsetRaw);
    } catch {
      throw new CliUsageError(`--set offset must be an integer, got "${entry}"`);
    }
    return { topic, partition, offset };
  });
}

function groupSetEntriesByTopic(
  entries: readonly SetEntry[],
): { topicName: string; partitions: { partitionIndex: number; startOffset: bigint }[] }[] {
  const byTopic = new Map<string, { partitionIndex: number; startOffset: bigint }[]>();
  for (const entry of entries) {
    const partitions = byTopic.get(entry.topic) ?? [];
    partitions.push({ partitionIndex: entry.partition, startOffset: entry.offset });
    byTopic.set(entry.topic, partitions);
  }
  return [...byTopic.entries()].map(([topicName, partitions]) => ({ topicName, partitions }));
}

export const shareGroupOffsetsCommand: CommandSpec = {
  path: ['share-group', 'offsets'],
  summary: 'Read, set, or delete a share group’s committed start offsets',
  flags: [
    { name: 'brokers', type: 'string', brief: 'comma-separated broker list, e.g. localhost:9092' },
    { name: 'topic', type: 'string', multiple: true, brief: 'limit a read to these topics (repeatable)' },
    {
      name: 'set',
      type: 'string',
      multiple: true,
      brief: '"topic:partition:offset" to set as the new start offset (repeatable)',
    },
    {
      name: 'delete-topic',
      type: 'string',
      multiple: true,
      brief: 'delete committed offsets for this topic (repeatable)',
    },
    { name: 'yes', type: 'boolean', brief: 'confirm --set/--delete-topic without an interactive prompt' },
  ],
  positionals: [{ name: 'groupId', brief: 'share group id' }],
  examples: [
    'share-group offsets orders-readers --brokers localhost:9092',
    'share-group offsets orders-readers --set orders:0:1000 --yes --brokers localhost:9092',
    'share-group offsets orders-readers --delete-topic orders --yes --brokers localhost:9092',
  ],
  exitCodes: [EXIT_CODES.ok, EXIT_CODES.operationFailed, EXIT_CODES.usage, EXIT_CODES.abortedOrUnconfirmed],
  async run({ flags, positionals, runtime, output, config }) {
    const groupId = positionals[0];
    if (groupId === undefined) {
      throw new CliUsageError('share-group offsets requires a group id');
    }

    const setFlags = flags.set as string[] | undefined;
    const deleteTopics = flags['delete-topic'] as string[] | undefined;
    const modes = [
      setFlags !== undefined && setFlags.length > 0,
      deleteTopics !== undefined && deleteTopics.length > 0,
    ].filter(Boolean).length;
    if (modes > 1) {
      throw new CliUsageError('share-group offsets accepts at most one of --set or --delete-topic');
    }

    const brokers = parseBrokersFlag(flags.brokers);

    if (setFlags !== undefined && setFlags.length > 0) {
      const topics = groupSetEntriesByTopic(parseSetFlags(setFlags));
      await confirmDestructive({
        runtime,
        yes: flags.yes === true,
        message: `Set share group "${groupId}"'s start offsets on ${setFlags.join(', ')}?`,
        confirmDestructive: config.cli.confirmDestructive,
      });

      const admin = await runtime.openAdmin({ brokers, env: runtime.env, config });
      try {
        const { responses } = await admin.alterShareGroupOffsets({ groupId, topics });
        output.write({
          human: () =>
            renderTable(
              ['TOPIC', 'PARTITION', 'STATUS'],
              responses.flatMap((topicResult) =>
                topicResult.partitions.map((p) => [
                  topicResult.topicName,
                  String(p.partitionIndex),
                  p.errorCode === 0 ? 'set' : (p.errorMessage ?? `failed (code ${String(p.errorCode)})`),
                ]),
              ),
            ),
          json: () => stringifyJsonSafe({ responses }),
        });
        const allOk = responses.every((t) => t.partitions.every((p) => p.errorCode === 0));
        return allOk ? EXIT_CODES.ok : EXIT_CODES.operationFailed;
      } finally {
        await admin.disconnect();
      }
    }

    if (deleteTopics !== undefined && deleteTopics.length > 0) {
      await confirmDestructive({
        runtime,
        yes: flags.yes === true,
        message: `Delete share group "${groupId}"'s committed offsets on ${deleteTopics.join(', ')}?`,
        confirmDestructive: config.cli.confirmDestructive,
      });

      const admin = await runtime.openAdmin({ brokers, env: runtime.env, config });
      try {
        const { responses } = await admin.deleteShareGroupOffsets({ groupId, topics: deleteTopics });
        output.write({
          human: () =>
            renderTable(
              ['TOPIC', 'STATUS'],
              responses.map((r) => [
                r.topicName,
                r.errorCode === 0 ? 'deleted' : (r.errorMessage ?? `failed (code ${String(r.errorCode)})`),
              ]),
            ),
          json: () => stringifyJsonSafe({ responses }),
        });
        const allOk = responses.every((r) => r.errorCode === 0);
        return allOk ? EXIT_CODES.ok : EXIT_CODES.operationFailed;
      } finally {
        await admin.disconnect();
      }
    }

    const topics = flags.topic as string[] | undefined;
    const admin = await runtime.openAdmin({ brokers, env: runtime.env, config });
    try {
      const { groups } = await admin.listShareGroupOffsets({
        groups: [{ groupId, topics: topics?.map((topicName) => ({ topicName })) }],
      });

      const rows = groups.flatMap((group) =>
        group.topics.flatMap((topic) =>
          topic.partitions.map((p) => [
            topic.topicName,
            String(p.partitionIndex),
            p.startOffset.toString(),
            p.lag.toString(),
          ]),
        ),
      );

      output.write({
        human: () =>
          rows.length === 0 ? '(no offsets)' : renderTable(['TOPIC', 'PARTITION', 'START OFFSET', 'LAG'], rows),
        json: () => stringifyJsonSafe({ groups }),
      });
      return EXIT_CODES.ok;
    } finally {
      await admin.disconnect();
    }
  },
};
