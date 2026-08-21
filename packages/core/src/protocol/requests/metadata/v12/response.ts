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
  uuid,
} from '../../../schema';
import type { ResponseDefinition } from '../../../schema';
import { checkTopicMetadataErrors } from '../shared';
import type { MetadataResponseV11Body } from '../v11/response';

export type MetadataResponseV12Body = Omit<MetadataResponseV11Body, 'topicMetadata'> & {
  topicMetadata: Array<Omit<MetadataResponseV11Body['topicMetadata'][number], 'topic'> & { topic: string | null }>;
};

/**
 * Metadata Response (Version: 12) => throttle_time_ms [brokers] cluster_id controller_id [topics]
 *                                    TAG_BUFFER
 *   topics => error_code name topic_id is_internal [partitions] topic_authorized_operations TAG_BUFFER
 *     name => COMPACT_NULLABLE_STRING
 *
 * Topic `name` is nullable (null when queried by id and missing). Name is never null when
 * the topic-level error code is 0. Throttle semantics stay the v6/KIP-219 client-side meaning.
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
  field('topic', compactNullableString),
  field('topicId', uuid),
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
]);

const raw = defineResponse({ schema: bodySchema });

export const metadataResponseV12: ResponseDefinition<MetadataResponseV12Body> = {
  decode: async (rawData) => {
    const decoded = await raw.decode(rawData);
    return { ...decoded, throttleTime: 0, clientSideThrottleTime: decoded.throttleTime };
  },
  parse: async (data) => {
    checkTopicMetadataErrors(data.topicMetadata);
    return data;
  },
};
