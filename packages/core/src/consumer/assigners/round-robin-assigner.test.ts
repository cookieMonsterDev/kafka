import { describe, expect, it } from 'vitest';
import type { Cluster } from '../../cluster/index.js';
import { MemberAssignment, MemberMetadata } from '../assigner-protocol.js';
import { roundRobin } from './round-robin-assigner.js';

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
});
