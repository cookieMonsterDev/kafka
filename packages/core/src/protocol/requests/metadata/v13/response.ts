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
import { createErrorFromCode, failure } from '../../../error-codes';
import { checkTopicMetadataErrors } from '../shared';
import type { MetadataResponseV12Body } from '../v12/response';

export type MetadataResponseV13Body = MetadataResponseV12Body & {
  errorCode: number;
};

/**
 * Metadata Response (Version: 13) => throttle_time_ms [brokers] cluster_id controller_id [topics]
 *                                    error_code TAG_BUFFER
 *   topics => error_code name topic_id is_internal [partitions] topic_authorized_operations TAG_BUFFER
 *     name => COMPACT_NULLABLE_STRING
 *
 * Adds a top-level `errorCode`. Topic `name` remains nullable as of v12. Throttle semantics
 * stay the v6/KIP-219 client-side meaning.
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
  field('errorCode', int16),
]);

const raw = defineResponse({ schema: bodySchema });

export const metadataResponseV13: ResponseDefinition<MetadataResponseV13Body> = {
  decode: async (rawData) => {
    const decoded = await raw.decode(rawData);
    return { ...decoded, throttleTime: 0, clientSideThrottleTime: decoded.throttleTime };
  },
  parse: async (data) => {
    if (failure(data.errorCode)) {
      throw createErrorFromCode(data.errorCode);
    }
    checkTopicMetadataErrors(data.topicMetadata);
    return data;
  },
};
