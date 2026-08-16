import { describe, expect, it } from 'vitest';
import type { Cluster } from '../../cluster/index';
import { MemberAssignment, MemberMetadata } from '../assigner-protocol';
import { roundRobin } from './round-robin-assigner';

describe('consumer/assigners/round-robin-assigner', () => {
  const metadata: Record<string, { partitionId: number }[]> = {};
  const cluster = { findTopicPartitionMetadata: (topic: string) => metadata[topic] ?? [] } as unknown as Cluster;
  const assigner = roundRobin({ cluster, groupId: 'g', logger: {} as never });
  const topics = ['topic-A', 'topic-B'];

  it('assigns all topic-partitions evenly', async () => {
    metadata['topic-A'] = Array.from({ length: 14 }, (_, i) => ({ partitionId: i }));
    metadata['topic-B'] = Array.from({ length: 5 }, (_, i) => ({ partitionId: i }));

    const members = [
      { memberId: 'member-3' },
      { memberId: 'member-1' },
      { memberId: 'member-4' },
      { memberId: 'member-2' },
    ].map(({ memberId }) => ({ memberId, memberMetadata: Buffer.alloc(0) }));

    const assignment = await assigner.assign({ members, topics });

    expect(assignment).toEqual([
      {
        memberId: 'member-1',
        memberAssignment: MemberAssignment.encode({
          version: assigner.version,
          assignment: { 'topic-A': [0, 4, 8, 12], 'topic-B': [2] },
        }),
      },
      {
        memberId: 'member-2',
        memberAssignment: MemberAssignment.encode({
          version: assigner.version,
          assignment: { 'topic-A': [1, 5, 9, 13], 'topic-B': [3] },
        }),
      },
      {
        memberId: 'member-3',
        memberAssignment: MemberAssignment.encode({
          version: assigner.version,
          assignment: { 'topic-A': [2, 6, 10], 'topic-B': [0, 4] },
        }),
      },
      {
        memberId: 'member-4',
        memberAssignment: MemberAssignment.encode({
          version: assigner.version,
          assignment: { 'topic-A': [3, 7, 11], 'topic-B': [1] },
        }),
      },
    ]);
  });

  it('assigns topics with names taken from builtin functions', async () => {
    const builtinTopics = ['shift', 'toString'];
    metadata['shift'] = [{ partitionId: 0 }];
    metadata['toString'] = [{ partitionId: 0 }];

    const assignment = await assigner.assign({
      members: [{ memberId: 'member-1', memberMetadata: Buffer.alloc(0) }],
      topics: builtinTopics,
    });

    expect(assignment).toEqual([
      {
        memberId: 'member-1',
        memberAssignment: MemberAssignment.encode({
          version: assigner.version,
          assignment: { shift: [0], toString: [0] },
        }),
      },
    ]);
  });

  it('returns the assigner name and metadata from protocol()', () => {
    expect(assigner.protocol({ topics })).toEqual({
      name: assigner.name,
      metadata: MemberMetadata.encode({ version: assigner.version, topics }),
    });
  });

  it('assigns a partition only to members whose metadata lists that topic', async () => {
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
