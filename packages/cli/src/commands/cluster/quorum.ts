import type { DescribeMetadataQuorumResult } from '@cookiemonsterdev/kafka-core';
import { parseBrokersFlag } from '../../admin/parse-brokers';
import type { CommandSpec } from '../../args/define';
import { EXIT_CODES } from '../../errors/exit-codes';
import { stringifyJsonSafe } from '../../output/json';
import { renderTable } from '../../output/table';

// `MetadataQuorumReplica` isn't exported from core's package root (only `DescribeMetadataQuorumResult`
// is), so it's derived by indexed access — matching `acl/enums.ts`'s precedent for the same
// constraint — rather than reaching into core's internal module layout.
type MetadataQuorumReplica =
  DescribeMetadataQuorumResult['topics'][number]['partitions'][number]['currentVoters'][number];

function formatReplicas(replicas: readonly MetadataQuorumReplica[]): string {
  if (replicas.length === 0) return '(none)';
  return replicas.map((replica) => String(replica.replicaId)).join(', ');
}

export const clusterQuorumCommand: CommandSpec = {
  path: ['cluster', 'quorum'],
  summary: 'Describe the metadata quorum: leader, voters, and observers',
  flags: [{ name: 'brokers', type: 'string', brief: 'comma-separated broker list, e.g. localhost:9092' }],
  examples: ['cluster quorum --brokers localhost:9092'],
  exitCodes: [EXIT_CODES.ok, EXIT_CODES.operationFailed, EXIT_CODES.usage],
  async run({ flags, runtime, output, config }) {
    const brokers = parseBrokersFlag(flags.brokers);
    const admin = await runtime.openAdmin({ brokers, env: runtime.env, config });
    try {
      const quorum = await admin.describeMetadataQuorum();

      const rows = quorum.topics.flatMap((topic) =>
        topic.partitions.map((partition) => [
          topic.topicName,
          String(partition.partitionIndex),
          String(partition.leaderId),
          String(partition.leaderEpoch),
          partition.highWatermark.toString(),
          formatReplicas(partition.currentVoters),
          formatReplicas(partition.observers),
        ]),
      );

      output.write({
        human: () =>
          rows.length === 0
            ? '(no metadata quorum topics)'
            : renderTable(
                ['TOPIC', 'PARTITION', 'LEADER', 'LEADER_EPOCH', 'HIGH_WATERMARK', 'VOTERS', 'OBSERVERS'],
                rows,
              ),
        json: () => stringifyJsonSafe(quorum),
      });

      return EXIT_CODES.ok;
    } finally {
      await admin.disconnect();
    }
  },
};
