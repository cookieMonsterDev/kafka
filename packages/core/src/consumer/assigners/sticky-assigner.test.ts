import { describe, expect, it } from 'vitest';
import type { Cluster } from '../../cluster/index';
import { Encoder } from '../../protocol/encoder';
import { MemberAssignment, MemberMetadata } from '../assigner-protocol';
import { decodeStickyUserData, sticky } from './sticky-assigner';

describe('consumer/assigners/sticky-assigner', () => {
  const metadata: Record<string, { partitionId: number }[]> = {};
  const cluster = { findTopicPartitionMetadata: (topic: string) => metadata[topic] ?? [] } as unknown as Cluster;
  const assigner = sticky({ cluster, groupId: 'g', logger: {} as never });
  const topics = ['topic-A', 'topic-B'];

  function decodeGroup(assignment: { memberId: string; memberAssignment: Buffer }[]) {
    return Object.fromEntries(
      assignment.map(({ memberId, memberAssignment }) => [
        memberId,
        MemberAssignment.decode(memberAssignment)?.assignment ?? {},
      ]),
    );
  }

  function flatten(assignment: Record<string, number[]> | undefined): string[] {
    const keys: string[] = [];
    for (const topic of Object.keys(assignment ?? {}).sort()) {
      for (const partition of [...(assignment?.[topic] ?? [])].sort((a, b) => a - b)) {
        keys.push(`${topic}:${partition}`);
      }
    }
    return keys;
  }

  function userDataFor(assignment: Record<string, number[]>): Buffer {
    const assignedTopics = Object.keys(assignment).filter((topic) => (assignment[topic]?.length ?? 0) > 0);
    if (assignedTopics.length === 0) return Buffer.alloc(0);
    return new Encoder().writeArray(
      assignedTopics.map((topic) => new Encoder().writeString(topic).writeArray(assignment[topic] ?? [])),
    ).buffer;
  }

  it('assigns all topic-partitions evenly on the first join', async () => {
    metadata['topic-A'] = Array.from({ length: 14 }, (_, i) => ({ partitionId: i }));
    metadata['topic-B'] = Array.from({ length: 5 }, (_, i) => ({ partitionId: i }));

    const members = [
      { memberId: 'member-3' },
      { memberId: 'member-1' },
      { memberId: 'member-4' },
      { memberId: 'member-2' },
    ].map(({ memberId }) => ({ memberId, memberMetadata: Buffer.alloc(0) }));

    const decoded = decodeGroup(await assigner.assign({ members, topics }));
    const sizes = Object.keys(decoded)
      .sort()
      .map((memberId) => flatten(decoded[memberId]).length);
    const assigned = Object.values(decoded).flatMap((assignment) => flatten(assignment));

    expect(Object.keys(decoded).sort()).toEqual(['member-1', 'member-2', 'member-3', 'member-4']);
    expect(new Set(assigned).size).toBe(assigned.length);
    expect([...assigned].sort()).toEqual(
      [
        ...Array.from({ length: 14 }, (_, i) => `topic-A:${i}`),
        ...Array.from({ length: 5 }, (_, i) => `topic-B:${i}`),
      ].sort(),
    );
    expect(Math.max(...sizes) - Math.min(...sizes)).toBeLessThanOrEqual(1);
  });

  it('keeps most partitions when a new member joins with previous assignment in userData', async () => {
    metadata['topic-A'] = Array.from({ length: 6 }, (_, i) => ({ partitionId: i }));
    const topic = ['topic-A'];

    const firstMembers = ['member-1', 'member-2'].map((memberId) => ({
      memberId,
      memberMetadata: MemberMetadata.encode({ version: assigner.version, topics: topic }),
    }));
    const first = decodeGroup(await assigner.assign({ members: firstMembers, topics: topic }));

    expect(flatten(first['member-1']).length).toBe(3);
    expect(flatten(first['member-2']).length).toBe(3);

    const secondMembers = [
      {
        memberId: 'member-1',
        memberMetadata: MemberMetadata.encode({
          version: assigner.version,
          topics: topic,
          userData: userDataFor(first['member-1'] ?? {}),
        }),
      },
      {
        memberId: 'member-2',
        memberMetadata: MemberMetadata.encode({
          version: assigner.version,
          topics: topic,
          userData: userDataFor(first['member-2'] ?? {}),
        }),
      },
      {
        memberId: 'member-3',
        memberMetadata: MemberMetadata.encode({ version: assigner.version, topics: topic }),
      },
    ];

    const second = decodeGroup(await assigner.assign({ members: secondMembers, topics: topic }));
    const kept1 = flatten(second['member-1']).filter((key) => flatten(first['member-1']).includes(key));
    const kept2 = flatten(second['member-2']).filter((key) => flatten(first['member-2']).includes(key));

    expect(flatten(second['member-1']).length).toBe(2);
    expect(flatten(second['member-2']).length).toBe(2);
    expect(flatten(second['member-3']).length).toBe(2);
    expect(kept1.length).toBe(2);
    expect(kept2.length).toBe(2);
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
    const local = sticky({ cluster, groupId: 'g', logger: {} as never });
    expect(local.name).toBe('sticky');
    expect(local.protocolType).toBe('eager');
    expect(local.protocol({ topics })).toEqual({
      name: 'sticky',
      metadata: MemberMetadata.encode({ version: local.version, topics }),
    });
  });

  it('attaches this member previous assignment to protocol() after onAssignment', () => {
    const local = sticky({ cluster, groupId: 'g', logger: {} as never });
    const previous = { 'topic-A': [0, 2], 'topic-B': [1] };
    local.onAssignment?.(previous);

    const protocol = local.protocol({ topics });
    const decoded = MemberMetadata.decode(protocol.metadata);
    expect(protocol.name).toBe('sticky');
    expect(decoded.topics).toEqual(topics);
    expect(decodeStickyUserData(decoded.userData)).toEqual(previous);
  });
});
