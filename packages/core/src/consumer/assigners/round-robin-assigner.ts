import type { Cluster } from '../../cluster/index.js';
import { MemberAssignment, MemberMetadata } from '../assigner-protocol.js';
import type { Assigner, GroupMember, GroupMemberAssignment, GroupProtocol, PartitionAssigner } from '../types.js';

/**
 * Spreads topic-partitions across members in member-id order. `Object.create(null)` keeps topic
 * names that collide with `Object.prototype` (e.g. `toString`) from being inherited as methods.
 */
export const roundRobin: PartitionAssigner = ({ cluster }: { cluster: Cluster }): Assigner => {
  const assigner: Assigner = {
    name: 'RoundRobinAssigner',
    version: 0,

    async assign({
      members,
      topics,
    }: {
      members: readonly GroupMember[];
      topics: readonly string[];
    }): Promise<GroupMemberAssignment[]> {
      const membersCount = members.length;
      const sortedMembers = members.map(({ memberId }) => memberId).sort();
      const assignment: Record<string, Record<string, number[]>> = Object.create(null) as Record<
        string,
        Record<string, number[]>
      >;

      const topicsPartitions = topics.flatMap((topic) => {
        const partitionMetadata = cluster.findTopicPartitionMetadata(topic);
        return partitionMetadata.map((m) => ({ topic, partitionId: m.partitionId }));
      });

      topicsPartitions.forEach((topicPartition, i) => {
        const assignee = sortedMembers[i % membersCount];
        if (assignee === undefined) return;

        assignment[assignee] ??= Object.create(null) as Record<string, number[]>;
        const memberAssignment = assignment[assignee];
        memberAssignment[topicPartition.topic] ??= [];
        memberAssignment[topicPartition.topic]!.push(topicPartition.partitionId);
      });

      return Object.keys(assignment).map((memberId) => ({
        memberId,
        memberAssignment: MemberAssignment.encode({
          version: assigner.version,
          assignment: assignment[memberId] ?? {},
        }),
      }));
    },

    protocol({ topics }: { topics: readonly string[] }): GroupProtocol {
      return {
        name: assigner.name,
        metadata: MemberMetadata.encode({
          version: assigner.version,
          topics,
        }),
      };
    },
  };

  return assigner;
};
