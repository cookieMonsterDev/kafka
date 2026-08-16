import { array, boolean, defineResponse, field, int16, int32, nullableString, object, string } from '../../../schema';
import type { ResponseDefinition } from '../../../schema';
import { checkTopicMetadataErrors } from '../shared';

/**
 * Metadata Response (Version: 8) => throttle_time_ms [brokers] cluster_id controller_id [topics]
 *                                   cluster_authorized_operations
 *   throttle_time_ms => INT32
 *   brokers => node_id host port rack
 *     node_id => INT32
 *     host => STRING
 *     port => INT32
 *     rack => NULLABLE_STRING
 *   cluster_id => NULLABLE_STRING
 *   controller_id => INT32
 *   topics => error_code name is_internal [partitions] topic_authorized_operations
 *     error_code => INT16
 *     name => STRING
 *     is_internal => BOOLEAN
 *     partitions => error_code partition_index leader leader_epoch [replicas] [isr] [offline_replicas]
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
 * KIP-430 authorized-operations bitmasks. `-2147483648` (INT32_MIN) means the broker omitted them.
 * Throttle semantics stay the v6/KIP-219 client-side meaning.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const brokerSchema = object([
  field('nodeId', int32),
  field('host', string),
  field('port', int32),
  field('rack', nullableString),
]);
const partitionMetadataSchema = object([
  field('partitionErrorCode', int16),
  field('partitionId', int32),
  field('leader', int32),
  field('leaderEpoch', int32),
  field('replicas', array(int32)),
  field('isr', array(int32)),
  field('offlineReplicas', array(int32)),
]);
const topicMetadataSchema = object([
  field('topicErrorCode', int16),
  field('topic', string),
  field('isInternal', boolean),
  field('partitionMetadata', array(partitionMetadataSchema)),
  field('topicAuthorizedOperations', int32),
]);
const bodySchema = object([
  field('throttleTime', int32),
  field('brokers', array(brokerSchema)),
  field('clusterId', nullableString),
  field('controllerId', int32),
  field('topicMetadata', array(topicMetadataSchema)),
  field('clusterAuthorizedOperations', int32),
]);

const raw = defineResponse({ schema: bodySchema });

type RawBody = Awaited<ReturnType<typeof raw.decode>>;
export type MetadataResponseV8Body = Omit<RawBody, 'throttleTime'> & {
  throttleTime: number;
  clientSideThrottleTime: number;
};

export const metadataResponseV8: ResponseDefinition<MetadataResponseV8Body> = {
  decode: async (rawData) => {
    const decoded = await raw.decode(rawData);
    return { ...decoded, throttleTime: 0, clientSideThrottleTime: decoded.throttleTime };
  },
  parse: async (data) => {
    checkTopicMetadataErrors(data.topicMetadata);
    return data;
  },
};
