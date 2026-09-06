import { z } from 'zod';

/** Kafka's own topic-name rule: printable ASCII, `.`/`_`/`-` allowed, 249 bytes max. */
const TOPIC_NAME_PATTERN = /^[a-zA-Z0-9._-]+$/;

export const topicNameSchema = z
  .string()
  .min(1, 'topic name is required')
  .max(249, 'topic name must be at most 249 characters')
  .regex(TOPIC_NAME_PATTERN, 'topic name may only contain letters, digits, "." "_" and "-"');

export const topicConfigEntriesSchema = z.record(z.string().min(1), z.string());

export const createTopicRequestSchema = z.object({
  topic: topicNameSchema,
  numPartitions: z.number().int().positive().optional(),
  replicationFactor: z.number().int().positive().optional(),
  configEntries: topicConfigEntriesSchema.optional(),
});
export type CreateTopicRequest = z.infer<typeof createTopicRequestSchema>;

export const createPartitionsRequestSchema = z.object({
  count: z.number().int().positive(),
});
export type CreatePartitionsRequest = z.infer<typeof createPartitionsRequestSchema>;

export const alterTopicConfigsRequestSchema = z
  .object({
    set: topicConfigEntriesSchema.optional(),
    unset: z.array(z.string().min(1)).optional(),
  })
  .refine((value) => (value.set !== undefined && Object.keys(value.set).length > 0) || (value.unset?.length ?? 0) > 0, {
    message: 'at least one of "set" or "unset" is required',
  });
export type AlterTopicConfigsRequest = z.infer<typeof alterTopicConfigsRequestSchema>;

/** One partition's placement — shared by the list and detail views, the detail view adds offsets. */
export interface TopicPartitionSummary {
  readonly partitionIndex: number;
  readonly leader: number;
  readonly replicas: readonly number[];
  readonly isr: readonly number[];
}

export interface TopicListEntry {
  readonly name: string;
  readonly partitionCount: number;
  /** `null` when the topic has no partitions to read a replica count from. */
  readonly replicationFactor: number | null;
}

export interface TopicListResponse {
  readonly topics: readonly TopicListEntry[];
}

export interface TopicConfigEntry {
  readonly name: string;
  readonly value: string | null;
  readonly isDefault: boolean;
  readonly readOnly: boolean;
  readonly isSensitive: boolean;
}

export interface TopicPartitionDetail extends TopicPartitionSummary {
  /** Offsets and size are `bigint` on the server; all three serialize as decimal strings over the wire. */
  readonly earliestOffset: string | null;
  readonly latestOffset: string | null;
  /** `null` when no broker reported a log dir for this partition (e.g. it errored or is still electing a leader). */
  readonly sizeBytes: string | null;
}

export interface TopicDetailResponse {
  readonly name: string;
  readonly partitions: readonly TopicPartitionDetail[];
  readonly configs: readonly TopicConfigEntry[];
}
