import { array, defineResponse, field, int16, int32, object, string } from '../../../schema';
import { checkTopicMetadataErrors } from '../shared';

/**
 * Metadata Response (Version: 0) => [brokers] [topic_metadata]
 *   brokers => node_id host port
 *     node_id => INT32
 *     host => STRING
 *     port => INT32
 *   topic_metadata => topic_error_code topic [partition_metadata]
 *     topic_error_code => INT16
 *     topic => STRING
 *     partition_metadata => partition_error_code partition_id leader [replicas] [isr]
 *       partition_error_code => INT16
 *       partition_id => INT32
 *       leader => INT32
 *       replicas => INT32
 *       isr => INT32
 */
const brokerSchema = object([field('nodeId', int32), field('host', string), field('port', int32)]);
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
  field('partitionMetadata', array(partitionMetadataSchema)),
]);
const bodySchema = object([field('brokers', array(brokerSchema)), field('topicMetadata', array(topicMetadataSchema))]);

export const metadataResponseV0 = defineResponse({
  schema: bodySchema,
  parse: async (data) => {
    checkTopicMetadataErrors(data.topicMetadata);
    return data;
  },
});
