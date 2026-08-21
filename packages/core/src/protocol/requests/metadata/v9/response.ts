import {
  boolean,
  compactArray,
  compactNullableString,
  compactString,
  defineResponse,
  field,
  flexibleObject,
  int16,
  int32,
} from '../../../schema';
import type { ResponseDefinition } from '../../../schema';
import type { MetadataResponseV6Body } from '../v6/response';
import { checkTopicMetadataErrors } from '../shared';

/**
 * Metadata body for v9. Extra v7–v9 fields (`leaderEpoch`, `topicAuthorizedOperations`)
 * are present at runtime when those versions are negotiated; `clusterAuthorizedOperations`
 * is always on the v8–v10 wire. Topic IDs arrive in v10; Cluster stores a wider
 * `ClusterMetadata` shape so older bodies remain assignable.
 */
export type MetadataResponseV9Body = MetadataResponseV6Body & {
  clusterAuthorizedOperations: number;
};

/**
 * Metadata Response (Version: 9) => throttle_time_ms [brokers] cluster_id controller_id [topics]
 *                                   cluster_authorized_operations TAG_BUFFER
 *   throttle_time_ms => INT32
 *   brokers => node_id host port rack TAG_BUFFER
 *     node_id => INT32
 *     host => COMPACT_STRING
 *     port => INT32
 *     rack => COMPACT_NULLABLE_STRING
 *   cluster_id => COMPACT_NULLABLE_STRING
 *   controller_id => INT32
 *   topics => error_code name is_internal [partitions] topic_authorized_operations TAG_BUFFER
 *     error_code => INT16
 *     name => COMPACT_STRING
 *     is_internal => BOOLEAN
 *     partitions => error_code partition_index leader leader_epoch [replicas] [isr]
 *                   [offline_replicas] TAG_BUFFER
 *       error_code => INT16
 *       partition_index => INT32
 *       leader => INT32
 *       leader_epoch => INT32
 *       replicas => INT32
 *       isr => INT32
 *       offline_replicas => INT32
 *     topic_authorized_operations => INT32
 *   cluster_authorized_operations => INT32
 *
 * Flexible form of v8 (KIP-482). Topic IDs arrive in v10 and are not implemented here.
 * Response header v1's trailing TAG_BUFFER is skipped by `Connection` before `decode()` runs.
 * Throttle semantics stay the v6/KIP-219 client-side meaning.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const brokerSchema = flexibleObject([
  field('nodeId', int32),
  field('host', compactString),
  field('port', int32),
  field('rack', compactNullableString),
]);
const partitionMetadataSchema = flexibleObject([
  field('partitionErrorCode', int16),
  field('partitionId', int32),
  field('leader', int32),
  field('leaderEpoch', int32),
  field('replicas', compactArray(int32)),
  field('isr', compactArray(int32)),
  field('offlineReplicas', compactArray(int32)),
]);
const topicMetadataSchema = flexibleObject([
  field('topicErrorCode', int16),
  field('topic', compactString),
  field('isInternal', boolean),
  field('partitionMetadata', compactArray(partitionMetadataSchema)),
  field('topicAuthorizedOperations', int32),
]);
const bodySchema = flexibleObject([
  field('throttleTime', int32),
  field('brokers', compactArray(brokerSchema)),
  field('clusterId', compactNullableString),
  field('controllerId', int32),
  field('topicMetadata', compactArray(topicMetadataSchema)),
  field('clusterAuthorizedOperations', int32),
]);

const raw = defineResponse({ schema: bodySchema });

export const metadataResponseV9: ResponseDefinition<MetadataResponseV9Body> = {
  decode: async (rawData) => {
    const decoded = await raw.decode(rawData);
    return { ...decoded, throttleTime: 0, clientSideThrottleTime: decoded.throttleTime };
  },
  parse: async (data) => {
    checkTopicMetadataErrors(data.topicMetadata);
    return data;
  },
};
