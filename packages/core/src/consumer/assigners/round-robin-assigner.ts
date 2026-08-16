import type { Cluster } from '../../cluster/index';
import { MemberAssignment, MemberMetadata } from '../assigner-protocol';
import type { Assigner, GroupMember, GroupMemberAssignment, GroupProtocol, PartitionAssigner } from '../types';

function subscribedTopics(member: GroupMember, fallbackTopics: readonly string[]): string[] {
  if (!member.memberMetadata.length) {
    return [...fallbackTopics];
  }

  try {
    return MemberMetadata.decode(member.memberMetadata).topics;
  } catch {
    return [...fallbackTopics];
  }
}

/**
 * Spreads topic-partitions across members in member-id order, assigning a partition only to
 * members whose metadata lists that topic (Java `RoundRobinAssignor`).
 * @see https://kafka.apache.org/43/design/design/
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
      const sortedMembers = [...members].sort((a, b) => a.memberId.localeCompare(b.memberId));
      const memberIds = sortedMembers.map((member) => member.memberId);
      const topicsByMember = new Map(
        sortedMembers.map((member) => [member.memberId, subscribedTopics(member, topics)]),
      );
      const assignment: Record<string, Record<string, number[]>> = Object.create(null) as Record<
        string,
        Record<string, number[]>
      >;

      const topicsPartitions = topics.flatMap((topic) => {
        const partitionMetadata = cluster.findTopicPartitionMetadata(topic);
        return partitionMetadata.map((m) => ({ topic, partitionId: m.partitionId }));
      });

      let memberCursor = 0;
      for (const topicPartition of topicsPartitions) {
        if (memberIds.length === 0) break;

        let assignee: string | undefined;
        for (let skipped = 0; skipped < memberIds.length; skipped++) {
          const candidate = memberIds[memberCursor % memberIds.length];
          memberCursor += 1;
          if (candidate !== undefined && topicsByMember.get(candidate)?.includes(topicPartition.topic)) {
            assignee = candidate;
            break;
          }
        }
        if (assignee === undefined) continue;

        assignment[assignee] ??= Object.create(null) as Record<string, number[]>;
        const memberAssignment = assignment[assignee]!;
        memberAssignment[topicPartition.topic] ??= [];
        memberAssignment[topicPartition.topic]!.push(topicPartition.partitionId);
      }

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
