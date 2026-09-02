import { parseBrokersFlag } from '../../admin/parse-brokers';
import type { CommandSpec } from '../../args/define';
import { EXIT_CODES } from '../../errors/exit-codes';
import { stringifyJsonSafe } from '../../output/json';
import { renderTable } from '../../output/table';

export const clusterReassignListCommand: CommandSpec = {
  path: ['cluster', 'reassign', 'list'],
  summary: 'List every active partition reassignment',
  flags: [
    { name: 'brokers', type: 'string', brief: 'comma-separated broker list, e.g. localhost:9092' },
    { name: 'timeout', type: 'number', brief: 'request timeout in ms' },
  ],
  // No --topic filter, matching `kafka-reassign-partitions.sh --list` itself: the real tool takes
  // no topic-scoping flag for this action either.
  examples: ['cluster reassign list --brokers localhost:9092'],
  exitCodes: [EXIT_CODES.ok, EXIT_CODES.operationFailed, EXIT_CODES.usage],
  async run({ flags, runtime, output, config }) {
    const brokers = parseBrokersFlag(flags.brokers);
    const timeout = flags.timeout as number | undefined;
    const admin = await runtime.openAdmin({ brokers, env: runtime.env, config });
    try {
      const { topics } = await admin.listPartitionReassignments({ timeout });

      const rows = topics.flatMap((topic) =>
        topic.partitions.map((partition) => [
          topic.name,
          String(partition.partition),
          partition.replicas.join(', '),
          partition.addingReplicas.join(', ') || '(none)',
          partition.removingReplicas.join(', ') || '(none)',
        ]),
      );

      output.write({
        human: () =>
          rows.length === 0
            ? '(no active reassignments)'
            : renderTable(['TOPIC', 'PARTITION', 'REPLICAS', 'ADDING', 'REMOVING'], rows),
        json: () => stringifyJsonSafe({ topics }),
      });

      return EXIT_CODES.ok;
    } finally {
      await admin.disconnect();
    }
  },
};
