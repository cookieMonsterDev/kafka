import { Decoder } from '../../../decoder';
import { createErrorFromCode, failure } from '../../../error-codes';
import {
  compactArray,
  compactNullableString,
  compactString,
  field,
  flexibleObject,
  int16,
  int32,
} from '../../../schema';
import type { ResponseDefinition } from '../../../schema';

export interface DeleteTopicsResponseV5Body {
  throttleTime: number;
  clientSideThrottleTime: number;
  topicErrors: { topic: string; errorCode: number; errorMessage: string | null }[];
}

/**
 * DeleteTopics Response (Version: 5) => throttle_time_ms [responses] TAG_BUFFER
 *   throttle_time_ms => INT32
 *   responses => name error_code error_message TAG_BUFFER
 *     name => COMPACT_STRING
 *     error_code => INT16
 *     error_message => COMPACT_NULLABLE_STRING
 *
 * Adds per-topic ErrorMessage (KIP-599). May return THROTTLING_QUOTA_EXCEEDED.
 * Quota timing follows v1 (KIP-219).
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const topicErrorSchema = flexibleObject([
  field('topic', compactString),
  field('errorCode', int16),
  field('errorMessage', compactNullableString),
]);
const bodySchema = flexibleObject([field('throttleTime', int32), field('topicErrors', compactArray(topicErrorSchema))]);

const topicNameComparator = (a: { topic: string }, b: { topic: string }): number => a.topic.localeCompare(b.topic);

export const deleteTopicsResponseV5: ResponseDefinition<DeleteTopicsResponseV5Body> = {
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
