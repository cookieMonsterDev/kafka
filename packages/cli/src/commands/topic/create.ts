import { CliUsageError } from '../../args/coerce';
import type { CommandSpec } from '../../args/define';
import { parseBrokersFlag } from '../../admin/parse-brokers';
import { EXIT_CODES } from '../../errors/exit-codes';
import { stringifyJsonSafe } from '../../output/json';
import { renderTable } from '../../output/table';
import { buildTopicConfig } from './build-topic-config';
import { mapWithConcurrency } from './concurrency';

const CONCURRENCY = 8;

interface TopicResult {
  readonly topic: string;
  readonly ok: boolean;
  readonly detail?: string;
}

export const topicCreateCommand: CommandSpec = {
  path: ['topic', 'create'],
  summary: 'Create one or more topics',
  flags: [
    { name: 'brokers', type: 'string', brief: 'comma-separated broker list, e.g. localhost:9092' },
    { name: 'partitions', type: 'number', alias: 'p', brief: 'number of partitions' },
    { name: 'replication-factor', type: 'number', alias: 'r', brief: 'replication factor' },
    {
      name: 'replica-assignment',
      type: 'string',
      multiple: true,
      brief:
        'explicit partition=replica,replica assignment (repeatable, exclusive with --partitions/--replication-factor)',
    },
    {
      name: 'config',
      type: 'string',
      multiple: true,
      keyValue: true,
      brief: 'a topic config entry, key=value (repeatable)',
    },
    { name: 'dry-run', type: 'boolean', brief: 'validate without creating anything' },
    { name: 'if-not-exists', type: 'boolean', brief: 'treat an already-existing topic as success' },
    { name: 'fail-fast', type: 'boolean', brief: 'issue one batched call instead of one call per topic' },
  ],
  positionals: [{ name: 'topics', variadic: true, brief: 'topic names to create' }],
  examples: ['topic create orders --partitions 3 --replication-factor 1', 'topic create orders payments --dry-run'],
  exitCodes: [EXIT_CODES.ok, EXIT_CODES.operationFailed, EXIT_CODES.usage, EXIT_CODES.partialBatch],
  async run({ flags, positionals, runtime, output }) {
    if (positionals.length === 0) {
      throw new CliUsageError('topic create requires at least one topic name');
    }

    const brokers = parseBrokersFlag(flags.brokers);
    const dryRun = flags['dry-run'] === true;
    const ifNotExists = flags['if-not-exists'] === true;
    const failFast = flags['fail-fast'] === true;

    const topicFlags = {
      partitions: flags.partitions as number | undefined,
      replicationFactor: flags['replication-factor'] as number | undefined,
      replicaAssignment: flags['replica-assignment'] as string[] | undefined,
      config: flags.config as Record<string, string> | undefined,
    };
    const configs = positionals.map((topic) => buildTopicConfig(topic, topicFlags));

    const admin = await runtime.openAdmin({ brokers });
    try {
      let results: TopicResult[];

      if (configs.length === 1) {
        const created = await admin.createTopics({ topics: configs, validateOnly: dryRun });
        const ok = created || ifNotExists;
        results = [{ topic: configs[0]!.topic, ok, detail: ok ? undefined : 'already exists' }];
      } else if (failFast) {
        // core's createTopics() reports one boolean for the whole batch — `false` only means
        // "every failing topic in this call already existed", never which specific topics those
        // were, and a topic that genuinely got created leaves no positive trace either. Claiming
        // per-topic status here (e.g. "already exists" on a topic that may have just been
        // created) would be a false report, so a batched multi-topic call gets one honest,
        // combined result instead of N fabricated ones.
        const created = await admin.createTopics({ topics: configs, validateOnly: dryRun });
        const ok = created || ifNotExists;
        results = positionals.map((topic) => ({
          topic,
          ok,
          detail: ok
            ? undefined
            : 'batched call did not report full success; per-topic detail is unavailable with --fail-fast',
        }));
      } else {
        results = await mapWithConcurrency(configs, CONCURRENCY, async (config) => {
          try {
            const created = await admin.createTopics({ topics: [config], validateOnly: dryRun });
            const ok = created || ifNotExists;
            return { topic: config.topic, ok, detail: ok ? undefined : 'already exists' };
          } catch (error) {
            return { topic: config.topic, ok: false, detail: error instanceof Error ? error.message : String(error) };
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

      const okCount = results.filter((r) => r.ok).length;
      if (okCount === results.length) return EXIT_CODES.ok;
      if (okCount === 0) return EXIT_CODES.operationFailed;
      return EXIT_CODES.partialBatch;
    } finally {
      await admin.disconnect();
    }
  },
};
