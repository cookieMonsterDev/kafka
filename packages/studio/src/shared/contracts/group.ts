import { z } from 'zod';
import { topicNameSchema } from './topic';

export interface GroupListEntry {
  readonly groupId: string;
  readonly protocolType: string;
}

export interface GroupListResponse {
  readonly groups: readonly GroupListEntry[];
}

export interface GroupMember {
  readonly memberId: string;
  readonly instanceId: string | null;
  readonly clientId: string;
  readonly clientHost: string;
  readonly assignedTopicPartitions: readonly { readonly topic: string; readonly partitions: readonly number[] }[];
}

export interface GroupPartitionLag {
  readonly topic: string;
  readonly partition: number;
  /** `null` when the group has no committed offset on this partition yet. */
  readonly committedOffset: string | null;
  readonly logEndOffset: string;
  /** `null` alongside `committedOffset: null` — there is nothing to subtract from. */
  readonly lag: string | null;
}

export interface GroupDetailResponse {
  readonly groupId: string;
  readonly state: string;
  readonly protocolType: string;
  readonly assignorName: string;
  readonly members: readonly GroupMember[];
  readonly partitionLag: readonly GroupPartitionLag[];
}

const decimalOffsetSchema = z.string().regex(/^\d+$/, 'must be a non-negative integer');

/** One partition's reset target. `to` picks the reset kind; only the matching field is read. */
export const groupOffsetResetTargetSchema = z.discriminatedUnion('to', [
  z.object({ partition: z.number().int().nonnegative(), to: z.literal('earliest') }),
  z.object({ partition: z.number().int().nonnegative(), to: z.literal('latest') }),
  z.object({ partition: z.number().int().nonnegative(), to: z.literal('offset'), offset: decimalOffsetSchema }),
  z.object({ partition: z.number().int().nonnegative(), to: z.literal('timestamp'), timestamp: z.number().int() }),
]);
export type GroupOffsetResetTarget = z.infer<typeof groupOffsetResetTargetSchema>;

export const resetGroupOffsetsRequestSchema = z.object({
  topic: topicNameSchema,
  partitions: z.array(groupOffsetResetTargetSchema).min(1),
});
export type ResetGroupOffsetsRequest = z.infer<typeof resetGroupOffsetsRequestSchema>;

export interface ResetGroupOffsetsResponse {
  readonly groupId: string;
  readonly topic: string;
  readonly partitions: readonly { readonly partition: number; readonly offset: string }[];
}

export const deleteGroupOffsetsRequestSchema = z.object({
  topics: z
    .array(z.object({ topic: topicNameSchema, partitions: z.array(z.number().int().nonnegative()).min(1) }))
    .min(1),
});
export type DeleteGroupOffsetsRequest = z.infer<typeof deleteGroupOffsetsRequestSchema>;

export interface DeleteGroupOffsetsResponse {
  readonly groupId: string;
  readonly topics: readonly {
    readonly topic: string;
    readonly partitions: readonly { readonly partition: number; readonly errorCode: number }[];
  }[];
}

export const removeGroupMembersRequestSchema = z.object({
  members: z.array(z.object({ memberId: z.string().min(1), groupInstanceId: z.string().optional() })).min(1),
});
export type RemoveGroupMembersRequest = z.infer<typeof removeGroupMembersRequestSchema>;

export interface RemoveGroupMembersResult {
  readonly memberId: string;
  readonly groupInstanceId: string | null;
  readonly errorCode: number;
}

export interface RemoveGroupMembersResponse {
  readonly groupId: string;
  readonly members: readonly RemoveGroupMembersResult[];
}

export interface ShareGroupListResponse {
  readonly groups: readonly GroupListEntry[];
}

export interface ShareGroupPartitionOffset {
  readonly partition: number;
  readonly startOffset: string;
  readonly lag: string;
}

export interface ShareGroupDetailResponse {
  readonly groupId: string;
  readonly state: string;
  readonly members: readonly GroupMember[];
  readonly offsets: readonly { readonly topic: string; readonly partitions: readonly ShareGroupPartitionOffset[] }[];
}

export interface StreamsGroupListResponse {
  readonly groups: readonly GroupListEntry[];
}

/** A streams group member's tasks in one subtopology, one per partition of its source topics. */
export interface StreamsGroupTasks {
  readonly subtopologyId: string;
  readonly partitions: readonly number[];
}

export interface StreamsGroupMember {
  readonly memberId: string;
  /** The Kafka Streams instance the member belongs to; one instance can run several members. */
  readonly processId: string;
  readonly clientId: string;
  readonly clientHost: string;
  readonly activeTasks: readonly StreamsGroupTasks[];
  readonly standbyTasks: readonly StreamsGroupTasks[];
  readonly warmupTasks: readonly StreamsGroupTasks[];
}

export interface StreamsGroupSubtopology {
  readonly subtopologyId: string;
  readonly sourceTopics: readonly string[];
  readonly repartitionSinkTopics: readonly string[];
  readonly stateChangelogTopics: readonly string[];
  readonly repartitionSourceTopics: readonly string[];
}

export interface StreamsGroupDetailResponse {
  readonly groupId: string;
  readonly state: string;
  readonly groupEpoch: number;
  /** `null` before Kafka 4.4, which does not report the assignor. */
  readonly assignorName: string | null;
  /** `null` when the group has no topology yet. */
  readonly topologyEpoch: number | null;
  /** `null` while the topology is not ready, such as when its source topics are missing. */
  readonly subtopologies: readonly StreamsGroupSubtopology[] | null;
  readonly members: readonly StreamsGroupMember[];
}
