import { createErrorFromCode, failure } from '../../error-codes';

/**
 * Java `Integer.MIN_VALUE`. Brokers send this for `cluster_authorized_operations` /
 * `topic_authorized_operations` when those bitmasks were not computed (include flags false).
 */
export const AUTHORIZED_OPERATIONS_OMITTED = -2147483648;

/** All-zero UUID written when Metadata v10+ is queried by name (KIP-516). */
export const ZERO_TOPIC_ID = Buffer.alloc(16);

export interface MetadataRequestTopicEntry {
  name: string | null;
  topicId: Buffer;
}

/**
 * Name-only queries write a zero UUID so the v10+ topic struct is on the wire. Optional
 * `topicIds` are encoded with a null name (v12+ can look up by id; v10–v11 ignore them).
 * Empty input becomes a compact/nullable null array ("all topics").
 */
export function metadataRequestTopicEntries(
  topics: readonly string[] = [],
  topicIds: readonly Buffer[] = [],
): MetadataRequestTopicEntry[] | null {
  if (topics.length === 0 && topicIds.length === 0) return null;
  return [
    ...topics.map((name) => ({ name, topicId: ZERO_TOPIC_ID })),
    ...topicIds.map((topicId) => ({ name: null, topicId })),
  ];
}

/**
 * Metadata body stored by `Broker` / `Cluster` after any negotiated version.
 * `topicId` is present on v10+; `clusterAuthorizedOperations` is v8–v10 only;
 * topic `name` may be null on v12+; `errorCode` is v13+.
 */
export interface ClusterMetadataBroker {
  nodeId: number;
  host: string;
  port: number;
  rack: string | null;
}

export interface ClusterMetadataPartition {
  partitionErrorCode: number;
  partitionId: number;
  leader: number;
  leaderEpoch?: number;
  replicas: number[];
  isr: number[];
  offlineReplicas?: number[];
}

export interface ClusterMetadataTopic {
  topicErrorCode: number;
  topic: string | null;
  topicId?: Buffer;
  isInternal: boolean;
  partitionMetadata: ClusterMetadataPartition[];
  topicAuthorizedOperations?: number;
}

export interface ClusterMetadata {
  throttleTime: number;
  clientSideThrottleTime: number;
  brokers: ClusterMetadataBroker[];
  clusterId: string | null;
  controllerId: number;
  topicMetadata: ClusterMetadataTopic[];
  clusterAuthorizedOperations?: number;
  errorCode?: number;
}

/**
 * Shared Metadata error check: topic-level error first, then each partition's.
 * Each response version keeps its own body type and calls this helper.
 */
export interface TopicMetadataErrorShape {
  topicErrorCode: number;
  topic?: string | null;
  partitionMetadata: readonly { partitionErrorCode: number; partitionId?: number }[];
}

export function checkTopicMetadataErrors(topicMetadata: readonly TopicMetadataErrorShape[]): void {
  const topicWithError = topicMetadata.find((topic) => failure(topic.topicErrorCode));
  if (topicWithError) {
    throw createErrorFromCode(topicWithError.topicErrorCode, { topic: topicWithError.topic ?? undefined });
  }

  for (const topic of topicMetadata) {
    const partitionWithError = topic.partitionMetadata.find((partition) => failure(partition.partitionErrorCode));
    if (partitionWithError) {
      throw createErrorFromCode(partitionWithError.partitionErrorCode, {
        topic: topic.topic ?? undefined,
        partition: partitionWithError.partitionId,
      });
    }
  }
}
