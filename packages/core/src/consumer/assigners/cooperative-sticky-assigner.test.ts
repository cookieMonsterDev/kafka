import { describe, expect, it } from 'vitest';
import type { Cluster } from '../../cluster/index';
import { Encoder } from '../../protocol/encoder';
import { MemberAssignment, MemberMetadata } from '../assigner-protocol';
import { cooperativeSticky } from './cooperative-sticky-assigner';

describe('consumer/assigners/cooperative-sticky-assigner', () => {
  const metadata: Record<string, { partitionId: number }[]> = {};
  const cluster = { findTopicPartitionMetadata: (topic: string) => metadata[topic] ?? [] } as unknown as Cluster;
  const assigner = cooperativeSticky({ cluster, groupId: 'g', logger: {} as never });
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

    expect(new Set(assigned).size).toBe(assigned.length);
    expect(assigned.length).toBe(19);
    expect(Math.max(...sizes) - Math.min(...sizes)).toBeLessThanOrEqual(1);
  });

  it('revokes moving partitions on the first cooperative generation, then assigns them on the next', async () => {
    metadata['topic-A'] = Array.from({ length: 6 }, (_, i) => ({ partitionId: i }));
    const topic = ['topic-A'];

    const firstMembers = ['member-1', 'member-2'].map((memberId) => ({
      memberId,
      memberMetadata: MemberMetadata.encode({ version: assigner.version, topics: topic }),
    }));
    const first = decodeGroup(await assigner.assign({ members: firstMembers, topics: topic }));
    const firstOwned = new Set([...flatten(first['member-1']), ...flatten(first['member-2'])]);

    const joinMembers = [
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

    const incremental = decodeGroup(await assigner.assign({ members: joinMembers, topics: topic }));
    const incrementalOwned = [
      ...flatten(incremental['member-1']),
      ...flatten(incremental['member-2']),
      ...flatten(incremental['member-3']),
    ];

    expect(flatten(incremental['member-3'])).toEqual([]);
    expect(flatten(incremental['member-1']).length).toBe(2);
    expect(flatten(incremental['member-2']).length).toBe(2);
    expect(incrementalOwned.every((key) => firstOwned.has(key))).toBe(true);
    expect(flatten(incremental['member-1']).every((key) => flatten(first['member-1']).includes(key))).toBe(true);
    expect(flatten(incremental['member-2']).every((key) => flatten(first['member-2']).includes(key))).toBe(true);

    const settledMembers = [
      {
        memberId: 'member-1',
        memberMetadata: MemberMetadata.encode({
          version: assigner.version,
          topics: topic,
          userData: userDataFor(incremental['member-1'] ?? {}),
        }),
      },
      {
        memberId: 'member-2',
        memberMetadata: MemberMetadata.encode({
          version: assigner.version,
          topics: topic,
          userData: userDataFor(incremental['member-2'] ?? {}),
        }),
      },
      {
        memberId: 'member-3',
        memberMetadata: MemberMetadata.encode({
          version: assigner.version,
          topics: topic,
          userData: userDataFor(incremental['member-3'] ?? {}),
        }),
      },
    ];

    const settled = decodeGroup(await assigner.assign({ members: settledMembers, topics: topic }));
    expect(flatten(settled['member-1']).length).toBe(2);
    expect(flatten(settled['member-2']).length).toBe(2);
    expect(flatten(settled['member-3']).length).toBe(2);
    expect(flatten(settled['member-1']).every((key) => flatten(incremental['member-1']).includes(key))).toBe(true);
    expect(flatten(settled['member-2']).every((key) => flatten(incremental['member-2']).includes(key))).toBe(true);
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
    const local = cooperativeSticky({ cluster, groupId: 'g', logger: {} as never });
    expect(local.name).toBe('cooperative-sticky');
    expect(local.protocolType).toBe('cooperative');
    expect(local.protocol({ topics })).toEqual({
      name: 'cooperative-sticky',
      metadata: MemberMetadata.encode({ version: local.version, topics }),
    });
  });
});
