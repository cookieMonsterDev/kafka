import type { Cluster } from '../../cluster/index';
import { Decoder } from '../../protocol/decoder';
import { Encoder } from '../../protocol/encoder';
import { MemberAssignment, MemberMetadata } from '../assigner-protocol';
import type {
  Assigner,
  GroupMember,
  GroupMemberAssignment,
  GroupProtocol,
  MemberAssignment as MemberAssignmentMap,
  PartitionAssigner,
} from '../types';

interface TopicPartition {
  topic: string;
  partition: number;
}

interface StickyMember {
  memberId: string;
  subscribed: ReadonlySet<string>;
  previous: MemberAssignmentMap;
}

export interface StickyAssignerOptions {
  cluster: Cluster;
  name: string;
  protocolType: 'eager' | 'cooperative';
  cooperative: boolean;
}

function emptyAssignment(): MemberAssignmentMap {
  return Object.create(null) as MemberAssignmentMap;
}

function partitionKey(topic: string, partition: number): string {
  return `${topic}\0${partition}`;
}

/** Previous assignment in MemberAssignment topic/partitions array form (Java StickyAssignor userData V0). */
export function encodeStickyUserData(assignment: MemberAssignmentMap): Buffer {
  const topics = Object.keys(assignment)
    .filter((topic) => (assignment[topic]?.length ?? 0) > 0)
    .sort();
  if (topics.length === 0) {
    return Buffer.alloc(0);
  }

  return new Encoder().writeArray(
    topics.map((topic) =>
      new Encoder().writeString(topic).writeArray([...(assignment[topic] ?? [])].sort((a, b) => a - b)),
    ),
  ).buffer;
}

export function decodeStickyUserData(userData: Buffer): MemberAssignmentMap {
  const assignment = emptyAssignment();
  if (!userData.length) {
    return assignment;
  }

  try {
    const decoder = new Decoder(userData);
    decoder.readArray((entry) => {
      const topic = entry.readString();
      if (topic === null) throw new RangeError('Expected a non-null topic name, got null');
      assignment[topic] = entry.readArray((partitionDecoder) => partitionDecoder.readInt32());
      return topic;
    });
    return assignment;
  } catch {
    return emptyAssignment();
  }
}

function memberState(member: GroupMember, fallbackTopics: readonly string[]): StickyMember {
  if (!member.memberMetadata.length) {
    return { memberId: member.memberId, subscribed: new Set(fallbackTopics), previous: emptyAssignment() };
  }

  try {
    const decoded = MemberMetadata.decode(member.memberMetadata);
    return {
      memberId: member.memberId,
      subscribed: new Set(decoded.topics),
      previous: decodeStickyUserData(decoded.userData),
    };
  } catch {
    return { memberId: member.memberId, subscribed: new Set(fallbackTopics), previous: emptyAssignment() };
  }
}

function toAssignmentMap(partitions: readonly TopicPartition[]): MemberAssignmentMap {
  const assignment = emptyAssignment();
  for (const { topic, partition } of partitions) {
    assignment[topic] ??= [];
    assignment[topic].push(partition);
  }
  for (const topic of Object.keys(assignment)) {
    assignment[topic]!.sort((a, b) => a - b);
  }
  return assignment;
}

/**
 * AbstractStickyAssignor: keep valid previous partitions, fill unassigned, then steal only to restore
 * the balance constraint (no member more than one partition ahead of another who can take it).
 */
export function computeStickyAssignment({
  members,
  partitionsByTopic,
}: {
  members: readonly StickyMember[];
  partitionsByTopic: Map<string, number[]>;
}): Record<string, MemberAssignmentMap> {
  const sortedMembers = [...members].sort((a, b) => a.memberId.localeCompare(b.memberId));
  const memberIds = sortedMembers.map((member) => member.memberId);
  const subscribed = new Map(sortedMembers.map((member) => [member.memberId, member.subscribed]));
  const previousByMember = new Map(sortedMembers.map((member) => [member.memberId, member.previous]));

  const validPartitions = new Set<string>();
  const allPartitions: TopicPartition[] = [];
  for (const [topic, partitions] of partitionsByTopic) {
    for (const partition of [...partitions].sort((a, b) => a - b)) {
      const tp = { topic, partition };
      allPartitions.push(tp);
      validPartitions.add(partitionKey(topic, partition));
    }
  }

  const assigned = new Map<string, TopicPartition[]>();
  const ownerByPartition = new Map<string, string>();
  for (const memberId of memberIds) {
    assigned.set(memberId, []);
  }

  for (const member of sortedMembers) {
    for (const topic of Object.keys(member.previous)) {
      if (!member.subscribed.has(topic)) continue;
      for (const partition of member.previous[topic] ?? []) {
        const key = partitionKey(topic, partition);
        if (!validPartitions.has(key) || ownerByPartition.has(key)) continue;
        ownerByPartition.set(key, member.memberId);
        assigned.get(member.memberId)!.push({ topic, partition });
      }
    }
  }

  const unassigned = allPartitions
    .filter((tp) => !ownerByPartition.has(partitionKey(tp.topic, tp.partition)))
    .sort((a, b) => a.topic.localeCompare(b.topic) || a.partition - b.partition);

  const sizeOf = (memberId: string): number => assigned.get(memberId)?.length ?? 0;

  const eligibleFor = (topic: string): string[] => memberIds.filter((memberId) => subscribed.get(memberId)?.has(topic));

  for (const tp of unassigned) {
    const eligible = eligibleFor(tp.topic);
    if (eligible.length === 0) continue;
    eligible.sort((a, b) => sizeOf(a) - sizeOf(b) || a.localeCompare(b));
    const winner = eligible[0]!;
    assigned.get(winner)!.push(tp);
    ownerByPartition.set(partitionKey(tp.topic, tp.partition), winner);
  }

  const maxMoves = Math.max(allPartitions.length * Math.max(memberIds.length, 1), 1);
  for (let iteration = 0; iteration < maxMoves; iteration++) {
    const byLoad = [...memberIds].sort((a, b) => sizeOf(a) - sizeOf(b) || a.localeCompare(b));
    let moved = false;

    for (const minId of byLoad) {
      const minSubscribed = subscribed.get(minId);
      if (!minSubscribed) continue;

      for (let i = byLoad.length - 1; i >= 0; i--) {
        const maxId = byLoad[i]!;
        if (maxId === minId || sizeOf(maxId) - sizeOf(minId) <= 1) continue;

        const maxPartitions = assigned.get(maxId)!;
        const previousKeys = new Set<string>();
        const previous = previousByMember.get(minId) ?? emptyAssignment();
        for (const topic of Object.keys(previous)) {
          for (const partition of previous[topic] ?? []) {
            previousKeys.add(partitionKey(topic, partition));
          }
        }

        const stealable = maxPartitions.filter((tp) => minSubscribed.has(tp.topic));
        if (stealable.length === 0) continue;

        stealable.sort((a, b) => {
          const aPrev = previousKeys.has(partitionKey(a.topic, a.partition)) ? 0 : 1;
          const bPrev = previousKeys.has(partitionKey(b.topic, b.partition)) ? 0 : 1;
          return aPrev - bPrev || a.topic.localeCompare(b.topic) || a.partition - b.partition;
        });

        const tp = stealable[0]!;
        const maxList = assigned.get(maxId)!;
        const index = maxList.findIndex((entry) => entry.topic === tp.topic && entry.partition === tp.partition);
        if (index >= 0) maxList.splice(index, 1);
        assigned.get(minId)!.push(tp);
        ownerByPartition.set(partitionKey(tp.topic, tp.partition), minId);
        moved = true;
        break;
      }
      if (moved) break;
    }

    if (!moved) break;
  }

  const result: Record<string, MemberAssignmentMap> = Object.create(null) as Record<string, MemberAssignmentMap>;
  for (const memberId of memberIds) {
    result[memberId] = toAssignmentMap(assigned.get(memberId) ?? []);
  }
  return result;
}

function applyCooperativeConstraint(
  desired: Record<string, MemberAssignmentMap>,
  members: readonly StickyMember[],
): Record<string, MemberAssignmentMap> {
  const ownerByPartition = new Map<string, string>();
  for (const member of members) {
    for (const topic of Object.keys(member.previous)) {
      for (const partition of member.previous[topic] ?? []) {
        const key = partitionKey(topic, partition);
        if (!ownerByPartition.has(key)) ownerByPartition.set(key, member.memberId);
      }
    }
  }

  const result: Record<string, MemberAssignmentMap> = Object.create(null) as Record<string, MemberAssignmentMap>;
  for (const memberId of Object.keys(desired)) {
    const assigned = emptyAssignment();
    const desiredAssignment = desired[memberId] ?? emptyAssignment();
    for (const topic of Object.keys(desiredAssignment)) {
      const kept: number[] = [];
      for (const partition of desiredAssignment[topic] ?? []) {
        const owner = ownerByPartition.get(partitionKey(topic, partition));
        if (owner !== undefined && owner !== memberId) continue;
        kept.push(partition);
      }
      if (kept.length > 0) assigned[topic] = kept;
    }
    result[memberId] = assigned;
  }
  return result;
}

export function createStickyAssigner({ cluster, name, protocolType, cooperative }: StickyAssignerOptions): Assigner {
  let memberAssignment: MemberAssignmentMap = emptyAssignment();

  const assigner: Assigner = {
    name,
    version: 1,
    protocolType,

    async assign({
      members,
      topics,
    }: {
      members: readonly GroupMember[];
      topics: readonly string[];
    }): Promise<GroupMemberAssignment[]> {
      const stickyMembers = members.map((member) => memberState(member, topics));
      const partitionsByTopic = new Map<string, number[]>();
      for (const topic of topics) {
        partitionsByTopic.set(
          topic,
          cluster.findTopicPartitionMetadata(topic).map((metadata) => metadata.partitionId),
        );
      }

      let assignment = computeStickyAssignment({ members: stickyMembers, partitionsByTopic });
      if (cooperative) {
        assignment = applyCooperativeConstraint(assignment, stickyMembers);
      }

      return stickyMembers
        .map((member) => member.memberId)
        .sort()
        .map((memberId) => ({
          memberId,
          memberAssignment: MemberAssignment.encode({
            version: assigner.version,
            assignment: assignment[memberId] ?? emptyAssignment(),
          }),
        }));
    },

    protocol({ topics }: { topics: readonly string[] }): GroupProtocol {
      return {
        name: assigner.name,
        metadata: MemberMetadata.encode({
          version: assigner.version,
          topics,
          userData: encodeStickyUserData(memberAssignment),
        }),
      };
    },

    onAssignment(assignment: MemberAssignmentMap): void {
      const next = emptyAssignment();
      for (const topic of Object.keys(assignment)) {
        next[topic] = [...(assignment[topic] ?? [])];
      }
      memberAssignment = next;
    },
  };

  return assigner;
}

/**
 * Eager sticky assignor (Java `sticky`, KIP-54). Prefers keeping each member's previous partitions
 * while remaining balanced.
 * @see https://kafka.apache.org/43/design/design/
 */
export const sticky: PartitionAssigner = ({ cluster }: { cluster: Cluster }): Assigner =>
  createStickyAssigner({ cluster, name: 'sticky', protocolType: 'eager', cooperative: false });
