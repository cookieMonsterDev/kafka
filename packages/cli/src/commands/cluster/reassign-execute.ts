import { readJsonFile } from '../../admin/read-json-file';
import { parseBrokersFlag } from '../../admin/parse-brokers';
import { CliUsageError } from '../../args/coerce';
import type { CommandSpec } from '../../args/define';
import { EXIT_CODES, exitForBatchResults } from '../../errors/exit-codes';
import { isKafkaAggregateError } from '../../errors/is-kafka-aggregate-error';
import { confirmDestructive } from '../../interaction/confirm';
import { stringifyJsonSafe } from '../../output/json';
import { renderTable } from '../../output/table';
import { parseReassignmentFile, type PartitionReassignment } from './reassignment-file';

interface PartitionResult {
  readonly topic: string;
  readonly partition: number;
  readonly ok: boolean;
  readonly detail?: string;
}

interface AlterPartitionReassignmentsFailure {
  readonly topic: string;
  readonly partition: number;
  readonly message: string;
}

const LOG_DIRS_IGNORED_NOTE =
  'note: this file\'s "log_dirs" entries are not applied — moving a replica between log dirs on the same broker needs a separate command';

function isAlterPartitionReassignmentsError(item: unknown): item is AlterPartitionReassignmentsFailure {
  return (
    typeof item === 'object' &&
    item !== null &&
    (item as { name?: unknown }).name === 'KafkaAlterPartitionReassignmentsError' &&
    typeof (item as { topic?: unknown }).topic === 'string' &&
    typeof (item as { partition?: unknown }).partition === 'number'
  );
}

function flattenTargets(topics: readonly PartitionReassignment[]): { topic: string; partition: number }[] {
  return topics.flatMap((entry) =>
    entry.partitionAssignment.map((assignment) => ({ topic: entry.topic, partition: assignment.partition })),
  );
}

export const clusterReassignExecuteCommand: CommandSpec = {
  path: ['cluster', 'reassign', 'execute'],
  summary: 'Execute a partition reassignment from a kafka-reassign-partitions.sh-shaped JSON file',
  flags: [
    { name: 'brokers', type: 'string', brief: 'comma-separated broker list, e.g. localhost:9092' },
    {
      name: 'from-file',
      type: 'string',
      brief: 'path to a JSON file in the kafka-reassign-partitions.sh --reassignment-json-file shape',
    },
    { name: 'timeout', type: 'number', brief: 'request timeout in ms' },
    { name: 'dry-run', type: 'boolean', brief: 'print the planned reassignment and exit without connecting' },
    { name: 'yes', type: 'boolean', brief: 'confirm the reassignment without an interactive prompt' },
  ],
  examples: ['cluster reassign execute --from-file reassignment.json --brokers localhost:9092 --yes'],
  exitCodes: [
    EXIT_CODES.ok,
    EXIT_CODES.operationFailed,
    EXIT_CODES.usage,
    EXIT_CODES.partialBatch,
    EXIT_CODES.abortedOrUnconfirmed,
  ],
  async run({ flags, runtime, output, config }) {
    const fromFile = flags['from-file'] as string | undefined;
    if (fromFile === undefined) {
      throw new CliUsageError('cluster reassign execute requires --from-file');
    }
    const { topics, hasLogDirs } = parseReassignmentFile(readJsonFile(fromFile, 'from-file'), 'from-file');
    const targets = flattenTargets(topics);

    const timeout = flags.timeout as number | undefined;
    const dryRun = flags['dry-run'] === true;

    if (dryRun) {
      output.write({
        human: () =>
          [
            ...(hasLogDirs ? [LOG_DIRS_IGNORED_NOTE, ''] : []),
            renderTable(
              ['TOPIC', 'PARTITION', 'REPLICAS'],
              topics.flatMap((entry) =>
                entry.partitionAssignment.map((assignment) => [
                  entry.topic,
                  String(assignment.partition),
                  assignment.replicas.join(', '),
                ]),
              ),
            ),
          ].join('\n'),
        json: () => stringifyJsonSafe({ topics, logDirsIgnored: hasLogDirs }),
      });
      return EXIT_CODES.ok;
    }

    await confirmDestructive({
      runtime,
      yes: flags.yes === true,
      message: `Reassign partitions for topic${topics.length > 1 ? 's' : ''} ${topics.map((t) => t.topic).join(', ')}?`,
      confirmDestructive: config.cli.confirmDestructive,
    });

    const brokers = parseBrokersFlag(flags.brokers);
    const admin = await runtime.openAdmin({ brokers, env: runtime.env, config });
    try {
      let results: PartitionResult[];

      try {
        await admin.alterPartitionReassignments({ topics, timeout });
        results = targets.map((target) => ({ ...target, ok: true }));
      } catch (error) {
        if (!isKafkaAggregateError(error)) throw error;

        const failures = new Map(
          error.errors
            .filter(isAlterPartitionReassignmentsError)
            .map((failure) => [`${failure.topic}:${String(failure.partition)}`, failure]),
        );
        results = targets.map((target) => {
          const failure = failures.get(`${target.topic}:${String(target.partition)}`);
          if (failure === undefined) return { ...target, ok: true };
          return { ...target, ok: false, detail: failure.message };
        });
      }

      output.write({
        human: () =>
          [
            ...(hasLogDirs ? [LOG_DIRS_IGNORED_NOTE, ''] : []),
            renderTable(
              ['TOPIC', 'PARTITION', 'STATUS'],
              results.map((r) => [r.topic, String(r.partition), r.ok ? 'submitted' : (r.detail ?? 'failed')]),
            ),
          ].join('\n'),
        json: () => stringifyJsonSafe({ results, logDirsIgnored: hasLogDirs }),
      });

      return exitForBatchResults(results, (r) => r.ok);
    } finally {
      await admin.disconnect();
    }
  },
};
