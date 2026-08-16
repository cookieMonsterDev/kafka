import { Decoder } from '../../../decoder';
import { createErrorFromCode, failure } from '../../../error-codes';
import { compactArray, compactNullableString, field, flexibleObject, int16, int32, uuid } from '../../../schema';
import type { ResponseDefinition } from '../../../schema';

export interface DeleteTopicsResponseV6Body {
  throttleTime: number;
  clientSideThrottleTime: number;
  topicErrors: { topic: string | null; topicId: Buffer; errorCode: number; errorMessage: string | null }[];
}

/**
 * DeleteTopics Response (Version: 6) => throttle_time_ms [responses] TAG_BUFFER
 *   throttle_time_ms => INT32
 *   responses => name topic_id error_code error_message TAG_BUFFER
 *     name => COMPACT_NULLABLE_STRING
 *     topic_id => UUID
 *     error_code => INT16
 *     error_message => COMPACT_NULLABLE_STRING
 *
 * Adds `topicId`. Topic `name` is nullable when deleting by ID. Quota timing follows v1 (KIP-219).
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const topicErrorSchema = flexibleObject([
  field('topic', compactNullableString),
  field('topicId', uuid),
  field('errorCode', int16),
  field('errorMessage', compactNullableString),
]);
const bodySchema = flexibleObject([field('throttleTime', int32), field('topicErrors', compactArray(topicErrorSchema))]);

const topicNameComparator = (a: { topic: string | null }, b: { topic: string | null }): number =>
  (a.topic ?? '').localeCompare(b.topic ?? '');

export const deleteTopicsResponseV6: ResponseDefinition<DeleteTopicsResponseV6Body> = {
  decode: async (rawData) => {
    const decoded = bodySchema.read(new Decoder(rawData));
    return {
      throttleTime: 0,
      clientSideThrottleTime: decoded.throttleTime,
      topicErrors: [...decoded.topicErrors].sort(topicNameComparator),
    };
  },
  parse: async (data) => {
    const topicsWithError = data.topicErrors.filter(({ errorCode }) => failure(errorCode));
    if (topicsWithError.length > 0) {
      const [first] = topicsWithError;
      if (first) throw createErrorFromCode(first.errorCode);
    }
    return data;
  },
};
