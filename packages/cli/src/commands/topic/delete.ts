import type { Admin } from '@cookiemonsterdev/kafka-core';
import { isUnknownTopicOrPartitionError } from '../../admin/protocol-error';
import { parseBrokersFlag } from '../../admin/parse-brokers';
import type { CommandSpec } from '../../args/define';
import { CliUsageError } from '../../args/coerce';
import { EXIT_CODES, exitForBatchResults } from '../../errors/exit-codes';
import { confirmDestructive, requireForce } from '../../interaction/confirm';
import { stringifyJsonSafe } from '../../output/json';
import { renderTable } from '../../output/table';
import { mapWithConcurrency } from '../../concurrency';

const CONCURRENCY = 8;

/** `kafka-topics.sh --delete` treats more than this many names in one call as worth a second thought. */
const MAX_TOPICS_WITHOUT_FORCE = 10;

interface TopicResult {
  readonly topic: string;
  readonly ok: boolean;
  readonly detail?: string;
}

function isInternalTopic(topic: string): boolean {
  return topic.startsWith('__');
}

async function deleteOne(admin: Admin, topic: string, ifExists: boolean): Promise<TopicResult> {
  try {
    await admin.deleteTopics({ topics: [topic] });
    return { topic, ok: true };
  } catch (error) {
    if (ifExists && isUnknownTopicOrPartitionError(error)) {
      return { topic, ok: true, detail: 'did not exist' };
    }
    throw error;
  }
}

export const topicDeleteCommand: CommandSpec = {
  path: ['topic', 'delete'],
  summary: 'Delete one or more topics',
  flags: [
    { name: 'brokers', type: 'string', brief: 'comma-separated broker list, e.g. localhost:9092' },
    { name: 'if-exists', type: 'boolean', brief: 'treat a missing topic as success' },
    { name: 'yes', type: 'boolean', brief: 'confirm the deletion without an interactive prompt' },
    { name: 'force', type: 'boolean', brief: 'override the internal-topic and batch-size safety checks' },
  ],
  positionals: [{ name: 'topics', variadic: true, brief: 'topic names to delete' }],
  examples: [
    'topic delete orders --brokers localhost:9092 --yes',
    'topic delete __consumer_offsets --brokers localhost:9092 --yes --force',
  ],
  exitCodes: [
    EXIT_CODES.ok,
    EXIT_CODES.operationFailed,
    EXIT_CODES.usage,
    EXIT_CODES.partialBatch,
    EXIT_CODES.abortedOrUnconfirmed,
  ],
  async run({ flags, positionals, runtime, output, config }) {
    if (positionals.length === 0) {
      throw new CliUsageError('topic delete requires at least one topic name');
    }

    const brokers = parseBrokersFlag(flags.brokers);
    const ifExists = flags['if-exists'] === true;
    const yes = flags.yes === true;
    const force = flags.force === true;

    const internalTopics = positionals.filter(isInternalTopic);
    if (internalTopics.length > 0) {
      requireForce({
        force,
        reason: `deleting internal topic${internalTopics.length > 1 ? 's' : ''} ${internalTopics.join(', ')}`,
      });
    }
    if (positionals.length > MAX_TOPICS_WITHOUT_FORCE) {
      requireForce({ force, reason: `deleting ${positionals.length} topics in one call` });
    }

    await confirmDestructive({
      runtime,
      yes,
      message: `Delete topic${positionals.length > 1 ? 's' : ''} ${positionals.join(', ')}?`,
      confirmDestructive: config.cli.confirmDestructive,
    });

    const admin = await runtime.openAdmin({ brokers, env: runtime.env, config });
    try {
      let results: TopicResult[];

      if (positionals.length === 1) {
        results = [await deleteOne(admin, positionals[0]!, ifExists)];
      } else {
        results = await mapWithConcurrency(positionals, CONCURRENCY, async (topic) => {
          try {
            return await deleteOne(admin, topic, ifExists);
          } catch (error) {
            return { topic, ok: false, detail: error instanceof Error ? error.message : String(error) };
          }
        });
      }

      output.write({
        human: () =>
          renderTable(
            ['TOPIC', 'STATUS'],
            results.map((r) => [r.topic, r.ok ? (r.detail ?? 'deleted') : (r.detail ?? 'failed')]),
          ),
        json: () => stringifyJsonSafe({ results }),
      });

      return exitForBatchResults(results, (r) => r.ok);
    } finally {
      await admin.disconnect();
    }
  },
};
