import { Decoder } from '../../../decoder';
import { createErrorFromCode, failure } from '../../../error-codes';
import { compactArray, compactString, field, flexibleObject, int16, int32 } from '../../../schema';
import type { ResponseDefinition } from '../../../schema';
import type { DeleteTopicsResponseV1Body } from '../v1/response';

export type DeleteTopicsResponseV4Body = DeleteTopicsResponseV1Body;

/**
 * DeleteTopics Response (Version: 4) => throttle_time_ms [responses] TAG_BUFFER
 *   throttle_time_ms => INT32
 *   responses => name error_code TAG_BUFFER
 *     name => COMPACT_STRING
 *     error_code => INT16
 *
 * Flexible compact form of v1–v3. Response header v1's trailing TAG_BUFFER is skipped by
 * `Connection` before `decode()` runs. Quota timing follows v1 (KIP-219).
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const topicErrorSchema = flexibleObject([field('topic', compactString), field('errorCode', int16)]);
const bodySchema = flexibleObject([field('throttleTime', int32), field('topicErrors', compactArray(topicErrorSchema))]);

const topicNameComparator = (a: { topic: string }, b: { topic: string }): number => a.topic.localeCompare(b.topic);

export const deleteTopicsResponseV4: ResponseDefinition<DeleteTopicsResponseV4Body> = {
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
