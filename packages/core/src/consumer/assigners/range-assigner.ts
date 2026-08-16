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
 * Assigns contiguous partition ranges per topic to members subscribed to that topic
 * (Java `RangeAssignor`).
 * @see https://kafka.apache.org/43/design/design/
 */
export const range: PartitionAssigner = ({ cluster }: { cluster: Cluster }): Assigner => {
  const assigner: Assigner = {
    name: 'RangeAssigner',
    version: 0,

    async assign({
      members,
      topics,
    }: {
      members: readonly GroupMember[];
      topics: readonly string[];
    }): Promise<GroupMemberAssignment[]> {
      const assignment: Record<string, Record<string, number[]>> = Object.create(null) as Record<
        string,
        Record<string, number[]>
      >;

      for (const topic of topics) {
        const interested = members
          .filter((member) => subscribedTopics(member, topics).includes(topic))
          .map((member) => member.memberId)
          .sort();
        if (interested.length === 0) continue;

        const partitions = cluster
          .findTopicPartitionMetadata(topic)
          .map((metadata) => metadata.partitionId)
          .sort((a, b) => a - b);

        const partitionsPerMember = Math.floor(partitions.length / interested.length);
        const membersWithExtra = partitions.length % interested.length;

        let cursor = 0;
        interested.forEach((memberId, index) => {
          const count = partitionsPerMember + (index < membersWithExtra ? 1 : 0);
          const slice = partitions.slice(cursor, cursor + count);
          cursor += count;
          if (slice.length === 0) return;

          assignment[memberId] ??= Object.create(null) as Record<string, number[]>;
          assignment[memberId][topic] = slice;
        });
      }

      return Object.keys(assignment)
        .sort()
        .map((memberId) => ({
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
