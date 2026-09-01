import type { Admin } from '@cookiemonsterdev/kafka-core';
import { parseBrokersFlag } from '../../admin/parse-brokers';
import { CliUsageError } from '../../args/coerce';
import type { CommandSpec } from '../../args/define';
import { EXIT_CODES } from '../../errors/exit-codes';
import { confirmDestructive } from '../../interaction/confirm';
import { stringifyJsonSafe } from '../../output/json';
import { renderTable } from '../../output/table';
import { mapWithConcurrency } from '../topic/concurrency';

const CONCURRENCY = 8;
const RESET_TARGETS = ['earliest', 'latest'] as const;
type ResetTarget = (typeof RESET_TARGETS)[number];

interface PreviewPartition {
  readonly partition: number;
  readonly newOffset: string;
}

interface TopicResult {
  readonly topic: string;
  readonly ok: boolean;
  readonly detail?: string;
  readonly partitions?: readonly PreviewPartition[];
}

function resolveTarget(raw: string | undefined): ResetTarget {
  if (raw === undefined) {
    throw new CliUsageError('group reset-offsets requires --to');
  }
  if (!(RESET_TARGETS as readonly string[]).includes(raw)) {
    throw new CliUsageError(`--to must be one of: ${RESET_TARGETS.join(', ')} (got "${raw}")`);
  }
  return raw as ResetTarget;
}

async function previewOne(admin: Admin, topic: string, to: ResetTarget): Promise<TopicResult> {
  const offsets = await admin.fetchTopicOffsets(topic);
  return {
    topic,
    ok: true,
    partitions: offsets.map((entry) => ({
      partition: entry.partition,
      newOffset: (to === 'earliest' ? entry.low : entry.high).toString(),
    })),
  };
}

async function resetOne(admin: Admin, groupId: string, topic: string, to: ResetTarget): Promise<TopicResult> {
  await admin.resetOffsets({ groupId, topic, earliest: to === 'earliest' });
  return { topic, ok: true };
}

function renderPreviewHuman(results: readonly TopicResult[]): string {
  const rows: string[][] = [];
  for (const result of results) {
    if (!result.ok) {
      rows.push([result.topic, '(error)', result.detail ?? 'failed']);
      continue;
    }
    for (const partition of result.partitions ?? []) {
      rows.push([result.topic, String(partition.partition), partition.newOffset]);
    }
  }
  if (rows.length === 0) return '(no partitions)';
  return renderTable(['TOPIC', 'PARTITION', 'NEW OFFSET'], rows);
}

function renderExecuteHuman(results: readonly TopicResult[]): string {
  return renderTable(
    ['TOPIC', 'STATUS'],
    results.map((result) => [result.topic, result.ok ? 'reset' : (result.detail ?? 'failed')]),
  );
}

export const groupResetOffsetsCommand: CommandSpec = {
  path: ['group', 'reset-offsets'],
  summary: "Reset a consumer group's committed offsets on one or more topics",
  flags: [
    { name: 'brokers', type: 'string', brief: 'comma-separated broker list, e.g. localhost:9092' },
    { name: 'topic', type: 'string', multiple: true, brief: 'topic to reset (repeatable; at least one required)' },
    { name: 'to', type: 'string', brief: 'reset target: earliest or latest' },
    { name: 'execute', type: 'boolean', brief: 'actually reset the offsets (without it, preview only)' },
    { name: 'yes', type: 'boolean', brief: 'confirm the reset without an interactive prompt' },
  ],
  positionals: [{ name: 'groupId', brief: 'group id' }],
  examples: [
    'group reset-offsets my-group --topic orders --to earliest --brokers localhost:9092',
    'group reset-offsets my-group --topic orders --to earliest --execute --yes --brokers localhost:9092',
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
      throw new CliUsageError('group reset-offsets requires a group id');
    }

    const topics = flags.topic as string[] | undefined;
    if (topics === undefined || topics.length === 0) {
      throw new CliUsageError('group reset-offsets requires at least one --topic');
    }

    const to = resolveTarget(flags.to as string | undefined);
    const execute = flags.execute === true;
    const yes = flags.yes === true;
    const brokers = parseBrokersFlag(flags.brokers);

    if (execute) {
      await confirmDestructive({
        runtime,
        yes,
        message: `Reset offsets for group "${groupId}" on topic${topics.length > 1 ? 's' : ''} ${topics.join(', ')} to ${to}?`,
        confirmDestructive: config.cli.confirmDestructive,
      });
    }

    const admin = await runtime.openAdmin({ brokers, env: runtime.env, config });
    try {
      let results: TopicResult[];

      if (topics.length === 1) {
        const topic = topics[0]!;
        results = [execute ? await resetOne(admin, groupId, topic, to) : await previewOne(admin, topic, to)];
      } else {
        results = await mapWithConcurrency(topics, CONCURRENCY, async (topic) => {
          try {
            return execute ? await resetOne(admin, groupId, topic, to) : await previewOne(admin, topic, to);
          } catch (error) {
            return { topic, ok: false, detail: error instanceof Error ? error.message : String(error) };
          }
        });
      }

      output.write({
        human: () => (execute ? renderExecuteHuman(results) : renderPreviewHuman(results)),
        json: () => stringifyJsonSafe(execute ? { results } : { topics: results }),
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
