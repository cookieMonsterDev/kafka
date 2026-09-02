import type { Admin } from '@cookiemonsterdev/kafka-core';
import { parseBrokersFlag } from '../../admin/parse-brokers';
import { CliUsageError, coerceNumber } from '../../args/coerce';
import type { CommandSpec } from '../../args/define';
import { EXIT_CODES } from '../../errors/exit-codes';
import { confirmDestructive } from '../../interaction/confirm';
import { stringifyJsonSafe } from '../../output/json';
import { renderTable } from '../../output/table';
import { mapWithConcurrency } from '../topic/concurrency';

const CONCURRENCY = 8;

interface TopicResult {
  readonly topic: string;
  readonly ok: boolean;
  readonly detail?: string;
}

interface OffsetPartition {
  readonly partitionIndex: number;
  readonly startOffset: bigint;
  readonly lag: bigint;
}

interface ReadResult extends TopicResult {
  readonly partitions?: readonly OffsetPartition[];
}

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
): Map<string, { partitionIndex: number; startOffset: bigint }[]> {
  const byTopic = new Map<string, { partitionIndex: number; startOffset: bigint }[]>();
  for (const entry of entries) {
    const partitions = byTopic.get(entry.topic) ?? [];
    partitions.push({ partitionIndex: entry.partition, startOffset: entry.offset });
    byTopic.set(entry.topic, partitions);
  }
  return byTopic;
}

/**
 * One `alterShareGroupOffsets` call per topic: the broker's response throws on the first
 * partition error code found across every topic in one call, discarding info about every other
 * topic — the same hazard `group delete-offsets` already works around for `deleteGroupOffsets`.
 */
async function setOffsetsForTopic(
  admin: Admin,
  groupId: string,
  topicName: string,
  partitions: { partitionIndex: number; startOffset: bigint }[],
): Promise<TopicResult> {
  await admin.alterShareGroupOffsets({ groupId, topics: [{ topicName, partitions }] });
  return { topic: topicName, ok: true };
}

/** One `deleteShareGroupOffsets` call per topic, for the same reason `setOffsetsForTopic` is. */
async function deleteOffsetsForTopic(admin: Admin, groupId: string, topic: string): Promise<TopicResult> {
  await admin.deleteShareGroupOffsets({ groupId, topics: [topic] });
  return { topic, ok: true };
}

/**
 * One `listShareGroupOffsets` call per topic: `describeShareGroupOffsets`'s response throws on
 * the first topic/partition error code found across every topic in one call, discarding every
 * other topic's data — the same hazard `setOffsetsForTopic`/`deleteOffsetsForTopic` work around,
 * so a bad `--topic` filter among several shouldn't blank out the ones that are fine.
 */
async function listOffsetsForTopic(admin: Admin, groupId: string, topicName: string): Promise<ReadResult> {
  const { groups } = await admin.listShareGroupOffsets({ groups: [{ groupId, topics: [{ topicName }] }] });
  const found = groups[0]?.topics[0];
  if (found === undefined) return { topic: topicName, ok: false, detail: 'broker returned no result' };
  return { topic: topicName, ok: true, partitions: found.partitions };
}

function renderResults(results: readonly TopicResult[]): string {
  return renderTable(
    ['TOPIC', 'STATUS'],
    results.map((r) => [r.topic, r.ok ? 'ok' : (r.detail ?? 'failed')]),
  );
}

function renderReadResults(results: readonly ReadResult[]): string {
  const rows = results.flatMap((r) => {
    if (!r.ok) return [[r.topic, '(error)', r.detail ?? 'failed', '']];
    if (r.partitions === undefined || r.partitions.length === 0) return [[r.topic, '(no offsets)', '', '']];
    return r.partitions.map((p) => [r.topic, String(p.partitionIndex), p.startOffset.toString(), p.lag.toString()]);
  });
  return rows.length === 0 ? '(no offsets)' : renderTable(['TOPIC', 'PARTITION', 'START OFFSET', 'LAG'], rows);
}

function exitForResults(results: readonly TopicResult[]): number {
  const okCount = results.filter((r) => r.ok).length;
  if (okCount === results.length) return EXIT_CODES.ok;
  if (okCount === 0) return EXIT_CODES.operationFailed;
  return EXIT_CODES.partialBatch;
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
  exitCodes: [
    EXIT_CODES.ok,
    EXIT_CODES.operationFailed,
    EXIT_CODES.usage,
    EXIT_CODES.partialBatch,
    EXIT_CODES.abortedOrUnconfirmed,
  ],
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
      const byTopic = groupSetEntriesByTopic(parseSetFlags(setFlags));
      await confirmDestructive({
        runtime,
        yes: flags.yes === true,
        message: `Set share group "${groupId}"'s start offsets on ${setFlags.join(', ')}?`,
        confirmDestructive: config.cli.confirmDestructive,
      });

      const admin = await runtime.openAdmin({ brokers, env: runtime.env, config });
      try {
        const entries = [...byTopic.entries()];
        let results: TopicResult[];
        if (entries.length === 1) {
          const [topicName, partitions] = entries[0]!;
          results = [await setOffsetsForTopic(admin, groupId, topicName, partitions)];
        } else {
          results = await mapWithConcurrency(entries, CONCURRENCY, async ([topicName, partitions]) => {
            try {
              return await setOffsetsForTopic(admin, groupId, topicName, partitions);
            } catch (error) {
              return { topic: topicName, ok: false, detail: error instanceof Error ? error.message : String(error) };
            }
          });
        }
        output.write({ human: () => renderResults(results), json: () => stringifyJsonSafe({ results }) });
        return exitForResults(results);
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
        let results: TopicResult[];
        if (deleteTopics.length === 1) {
          results = [await deleteOffsetsForTopic(admin, groupId, deleteTopics[0]!)];
        } else {
          results = await mapWithConcurrency(deleteTopics, CONCURRENCY, async (topic) => {
            try {
              return await deleteOffsetsForTopic(admin, groupId, topic);
            } catch (error) {
              return { topic, ok: false, detail: error instanceof Error ? error.message : String(error) };
            }
          });
        }
        output.write({ human: () => renderResults(results), json: () => stringifyJsonSafe({ results }) });
        return exitForResults(results);
      } finally {
        await admin.disconnect();
      }
    }

    const topics = flags.topic as string[] | undefined;
    const admin = await runtime.openAdmin({ brokers, env: runtime.env, config });
    try {
      let results: ReadResult[];

      if (topics === undefined || topics.length <= 1) {
        const { groups } = await admin.listShareGroupOffsets({
          groups: [{ groupId, topics: topics?.map((topicName) => ({ topicName })) }],
        });
        results = groups.flatMap((group) =>
          group.topics.map((topic) => ({ topic: topic.topicName, ok: true, partitions: topic.partitions })),
        );
      } else {
        results = await mapWithConcurrency(topics, CONCURRENCY, async (topicName) => {
          try {
            return await listOffsetsForTopic(admin, groupId, topicName);
          } catch (error) {
            return { topic: topicName, ok: false, detail: error instanceof Error ? error.message : String(error) };
          }
        });
      }

      output.write({
        human: () => renderReadResults(results),
        json: () => stringifyJsonSafe({ results }),
      });
      return exitForResults(results);
    } finally {
      await admin.disconnect();
    }
  },
};
