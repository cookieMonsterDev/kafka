import { readJsonFile } from '../../admin/read-json-file';
import { parseBrokersFlag } from '../../admin/parse-brokers';
import { CliUsageError, coerceNumber } from '../../args/coerce';
import type { CommandSpec } from '../../args/define';
import { EXIT_CODES } from '../../errors/exit-codes';
import { confirmDestructive, requireForce } from '../../interaction/confirm';
import { stringifyJsonSafe } from '../../output/json';
import { renderTable } from '../../output/table';
import { parseElectionFile, type TopicPartitions } from './election-file';
import { resolveElectionType } from './enums';

const UNCLEAN_ELECTION_TYPE = 1;
/** `kafka-leader-election.sh`'s "no request failure" case: the preferred replica is already leader. */
const ELECTION_NOT_NEEDED = 84;

function parseTopicPartitionFlags(raw: readonly string[]): TopicPartitions[] {
  const byTopic = new Map<string, number[]>();
  for (const entry of raw) {
    const separatorIndex = entry.lastIndexOf(':');
    if (separatorIndex <= 0 || separatorIndex === entry.length - 1) {
      throw new CliUsageError(`--topic-partition must be "topic:partition", got "${entry}"`);
    }
    const topic = entry.slice(0, separatorIndex);
    const partition = coerceNumber(entry.slice(separatorIndex + 1), 'topic-partition');
    if (!Number.isInteger(partition) || partition < 0) {
      throw new CliUsageError(
        `--topic-partition must be "topic:partition" with a non-negative partition, got "${entry}"`,
      );
    }
    const partitions = byTopic.get(topic) ?? [];
    if (!partitions.includes(partition)) partitions.push(partition);
    byTopic.set(topic, partitions);
  }
  return [...byTopic.entries()].map(([topic, partitions]) => ({ topic, partitions }));
}

function describeTarget(topicPartitions: TopicPartitions[] | null): string {
  if (topicPartitions === null) return '(all eligible partitions)';
  return topicPartitions
    .flatMap((entry) => entry.partitions.map((partition) => `${entry.topic}:${String(partition)}`))
    .join(', ');
}

export const clusterElectLeadersCommand: CommandSpec = {
  path: ['cluster', 'elect-leaders'],
  summary: 'Trigger a preferred or unclean leader election on one or more partitions',
  flags: [
    { name: 'brokers', type: 'string', brief: 'comma-separated broker list, e.g. localhost:9092' },
    { name: 'election-type', type: 'string', brief: 'preferred or unclean' },
    {
      name: 'topic-partition',
      type: 'string',
      multiple: true,
      brief: 'a "topic:partition" to elect a leader on (repeatable)',
    },
    { name: 'all-topic-partitions', type: 'boolean', brief: 'elect on every eligible partition' },
    { name: 'from-file', type: 'string', brief: 'path to a kafka-leader-election.sh --path-to-json-file JSON file' },
    { name: 'timeout', type: 'number', brief: 'request timeout in ms' },
    { name: 'dry-run', type: 'boolean', brief: 'print the election target and exit without connecting' },
    { name: 'yes', type: 'boolean', brief: 'confirm the election without an interactive prompt' },
    { name: 'force', type: 'boolean', brief: 'required for --election-type unclean, which can lose data' },
  ],
  examples: [
    'cluster elect-leaders --election-type preferred --all-topic-partitions --brokers localhost:9092 --yes',
    'cluster elect-leaders --election-type unclean --topic-partition orders:0 --force --yes --brokers localhost:9092',
  ],
  exitCodes: [EXIT_CODES.ok, EXIT_CODES.operationFailed, EXIT_CODES.usage, EXIT_CODES.abortedOrUnconfirmed],
  async run({ flags, runtime, output, config }) {
    const electionTypeFlag = flags['election-type'] as string | undefined;
    if (electionTypeFlag === undefined) {
      throw new CliUsageError('cluster elect-leaders requires --election-type');
    }
    const electionType = resolveElectionType(electionTypeFlag);

    const topicPartitionFlags = flags['topic-partition'] as string[] | undefined;
    const allTopicPartitions = flags['all-topic-partitions'] === true;
    const fromFile = flags['from-file'] as string | undefined;

    const sources = [
      topicPartitionFlags !== undefined && topicPartitionFlags.length > 0,
      allTopicPartitions,
      fromFile !== undefined,
    ].filter(Boolean).length;
    if (sources !== 1) {
      throw new CliUsageError(
        'cluster elect-leaders requires exactly one of --topic-partition, --all-topic-partitions, or --from-file',
      );
    }

    const topicPartitions: TopicPartitions[] | null = allTopicPartitions
      ? null
      : fromFile !== undefined
        ? parseElectionFile(readJsonFile(fromFile, 'from-file'), 'from-file')
        : parseTopicPartitionFlags(topicPartitionFlags!);

    const timeout = flags.timeout as number | undefined;
    const dryRun = flags['dry-run'] === true;

    if (dryRun) {
      const target = describeTarget(topicPartitions);
      output.write({
        human: () => `Would run a ${electionTypeFlag} election on: ${target}`,
        json: () => stringifyJsonSafe({ electionType, topicPartitions }),
      });
      return EXIT_CODES.ok;
    }

    if (electionType === UNCLEAN_ELECTION_TYPE) {
      requireForce({ force: flags.force === true, reason: 'an unclean leader election, which can lose data' });
    }

    await confirmDestructive({
      runtime,
      yes: flags.yes === true,
      message: `Elect leaders (${electionTypeFlag}) on: ${describeTarget(topicPartitions)}?`,
      confirmDestructive: config.cli.confirmDestructive,
    });

    const brokers = parseBrokersFlag(flags.brokers);
    const admin = await runtime.openAdmin({ brokers, env: runtime.env, config });
    try {
      const { results } = await admin.electLeaders({ topicPartitions, electionType, timeout });

      const rows = results.flatMap((topicResult) =>
        topicResult.partitions.map((partition) => [
          topicResult.topic,
          String(partition.partition),
          partition.errorCode === 0
            ? 'elected'
            : partition.errorCode === ELECTION_NOT_NEEDED
              ? 'not needed'
              : `failed (code ${String(partition.errorCode)})`,
        ]),
      );

      output.write({
        human: () =>
          rows.length === 0 ? '(no partitions elected)' : renderTable(['TOPIC', 'PARTITION', 'STATUS'], rows),
        json: () => stringifyJsonSafe({ results }),
      });

      return EXIT_CODES.ok;
    } finally {
      await admin.disconnect();
    }
  },
};
