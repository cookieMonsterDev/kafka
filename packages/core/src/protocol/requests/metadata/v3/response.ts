import {
  array,
  boolean,
  defineResponse,
  field,
  int16,
  int32,
  nullableString,
  object,
  string,
} from '../../../schema.js';
import { checkTopicMetadataErrors } from '../shared.js';

/**
 * Metadata Response (Version: 3) => throttle_time_ms [brokers] cluster_id controller_id [topic_metadata]
 *   throttle_time_ms => INT32
 *   brokers => node_id host port rack
 *     node_id => INT32
 *     host => STRING
 *     port => INT32
 *     rack => NULLABLE_STRING
 *   cluster_id => NULLABLE_STRING
 *   controller_id => INT32
 *   topic_metadata => error_code topic is_internal [partition_metadata]
 *     error_code => INT16
 *     topic => STRING
 *     is_internal => BOOLEAN
 *     partition_metadata => error_code partition leader [replicas] [isr]
 *       error_code => INT16
 *       partition => INT32
 *       leader => INT32
 *       replicas => INT32
 *       isr => INT32
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
  field('replicas', array(int32)),
  field('isr', array(int32)),
]);
const topicMetadataSchema = object([
  field('topicErrorCode', int16),
  field('topic', string),
  field('isInternal', boolean),
  field('partitionMetadata', array(partitionMetadataSchema)),
]);
const bodySchema = object([
  field('throttleTime', int32),
  field('brokers', array(brokerSchema)),
  field('clusterId', nullableString),
  field('controllerId', int32),
  field('topicMetadata', array(topicMetadataSchema)),
]);

export const metadataResponseV3 = defineResponse({
  schema: bodySchema,
  parse: async (data) => {
    checkTopicMetadataErrors(data.topicMetadata);
    return data;
  },
});
