import type { Admin } from '@cookiemonsterdev/kafka-core';
import { parseBrokersFlag } from '../../admin/parse-brokers';
import { CliUsageError } from '../../args/coerce';
import type { CommandSpec } from '../../args/define';
import { EXIT_CODES, exitForBatchResults } from '../../errors/exit-codes';
import { confirmDestructive } from '../../interaction/confirm';
import { stringifyJsonSafe } from '../../output/json';
import { renderTable } from '../../output/table';
import { mapWithConcurrency } from '../../concurrency';

const CONCURRENCY = 8;

interface TopicResult {
  readonly topic: string;
  readonly ok: boolean;
  readonly detail?: string;
}

/**
 * One `deleteGroupOffsets` call per topic: the broker's response throws on the first partition
 * error code found across every topic in one call, discarding info about every other topic — the
 * same "a bad topic among several shouldn't fail the whole batch" property every other fan-out
 * command here preserves, even though the underlying API could technically accept every topic in
 * one call.
 */
async function deleteOffsetsForTopic(
  admin: Admin,
  groupId: string,
  topic: string,
  explicitPartitions: readonly number[] | undefined,
): Promise<TopicResult> {
  const partitions =
    explicitPartitions !== undefined && explicitPartitions.length > 0
      ? [...explicitPartitions]
      : (await admin.fetchTopicOffsets(topic)).map((entry) => entry.partition);

  await admin.deleteGroupOffsets({ groupId, topics: [{ topic, partitions }] });
  return { topic, ok: true };
}

export const groupDeleteOffsetsCommand: CommandSpec = {
  path: ['group', 'delete-offsets'],
  summary: "Delete a consumer group's committed offsets on one or more topics",
  flags: [
    { name: 'brokers', type: 'string', brief: 'comma-separated broker list, e.g. localhost:9092' },
    { name: 'yes', type: 'boolean', brief: 'confirm the deletion without an interactive prompt' },
    { name: 'topic', type: 'string', multiple: true, brief: 'topic to clear (repeatable; at least one required)' },
    {
      name: 'partition',
      type: 'number',
      multiple: true,
      brief: 'partition to clear, applied to every --topic (repeatable; defaults to every partition of each topic)',
    },
  ],
  positionals: [{ name: 'groupId', brief: 'group id' }],
  examples: ['group delete-offsets my-group --topic orders --brokers localhost:9092 --yes'],
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
      throw new CliUsageError('group delete-offsets requires a group id');
    }

    const topics = flags.topic as string[] | undefined;
    if (topics === undefined || topics.length === 0) {
      throw new CliUsageError('group delete-offsets requires at least one --topic');
    }

    const partitions = flags.partition as number[] | undefined;
    const brokers = parseBrokersFlag(flags.brokers);
    const yes = flags.yes === true;

    await confirmDestructive({
      runtime,
      yes,
      message: `Delete offsets for group "${groupId}" on topic${topics.length > 1 ? 's' : ''} ${topics.join(', ')}?`,
      confirmDestructive: config.cli.confirmDestructive,
    });

    const admin = await runtime.openAdmin({ brokers, env: runtime.env, config });
    try {
      let results: TopicResult[];

      if (topics.length === 1) {
        results = [await deleteOffsetsForTopic(admin, groupId, topics[0]!, partitions)];
      } else {
        results = await mapWithConcurrency(topics, CONCURRENCY, async (topic) => {
          try {
            return await deleteOffsetsForTopic(admin, groupId, topic, partitions);
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

      return exitForBatchResults(results, (r) => r.ok);
    } finally {
      await admin.disconnect();
    }
  },
};
