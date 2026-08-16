import { describe, expect, it } from 'vitest';
import type { Cluster } from '../../cluster/index';
import { MemberAssignment, MemberMetadata } from '../assigner-protocol';
import { range } from './range-assigner';

describe('consumer/assigners/range-assigner', () => {
  const metadata: Record<string, { partitionId: number }[]> = {};
  const cluster = { findTopicPartitionMetadata: (topic: string) => metadata[topic] ?? [] } as unknown as Cluster;
  const assigner = range({ cluster, groupId: 'g', logger: {} as never });

  it('assigns contiguous partition ranges per topic', async () => {
    metadata['topic-A'] = Array.from({ length: 5 }, (_, i) => ({ partitionId: i }));
    metadata['topic-B'] = Array.from({ length: 3 }, (_, i) => ({ partitionId: i }));

    const members = ['member-2', 'member-1'].map((memberId) => ({
      memberId,
      memberMetadata: MemberMetadata.encode({ version: 0, topics: ['topic-A', 'topic-B'] }),
    }));

    const assignment = await assigner.assign({ members, topics: ['topic-A', 'topic-B'] });
    const decoded = Object.fromEntries(
      assignment.map(({ memberId, memberAssignment }) => [memberId, MemberAssignment.decode(memberAssignment)]),
    );

    expect(decoded['member-1']?.assignment).toEqual({ 'topic-A': [0, 1, 2], 'topic-B': [0, 1] });
    expect(decoded['member-2']?.assignment).toEqual({ 'topic-A': [3, 4], 'topic-B': [2] });
  });

  it('only assigns partitions to members subscribed to that topic', async () => {
    metadata['topic-A'] = [{ partitionId: 0 }, { partitionId: 1 }];
    metadata['topic-B'] = [{ partitionId: 0 }, { partitionId: 1 }];

    const members = [
      { memberId: 'member-1', memberMetadata: MemberMetadata.encode({ version: 0, topics: ['topic-A'] }) },
      { memberId: 'member-2', memberMetadata: MemberMetadata.encode({ version: 0, topics: ['topic-B'] }) },
    ];

    const assignment = await assigner.assign({ members, topics: ['topic-A', 'topic-B'] });
    const decoded = Object.fromEntries(
      assignment.map(({ memberId, memberAssignment }) => [memberId, MemberAssignment.decode(memberAssignment)]),
    );

    expect(decoded['member-1']?.assignment).toEqual({ 'topic-A': [0, 1] });
    expect(decoded['member-2']?.assignment).toEqual({ 'topic-B': [0, 1] });
  });
});
