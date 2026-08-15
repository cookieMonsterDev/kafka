import { Decoder } from '../../../decoder.js';
import { createErrorFromCode, failure } from '../../../error-codes.js';
import type { ResponseDefinition } from '../../../schema.js';
import { array, field, int16, int32, object, string } from '../../../schema.js';

export interface DeleteTopicsResponseV1Body {
  throttleTime: number;
  clientSideThrottleTime: number;
  topicErrors: { topic: string; errorCode: number }[];
}

/**
 * Starting in version 1, on quota violation, brokers send out responses before throttling.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-219+-+Improve+quota+communication
 *
 * DeleteTopics Response (Version: 1) => throttle_time_ms [topic_error_codes]
 *   throttle_time_ms => INT32
 *   topic_error_codes => topic error_code
 *     topic => STRING
 *     error_code => INT16
 */
const topicErrorSchema = object([field('topic', string), field('errorCode', int16)]);
const bodySchema = object([field('throttleTime', int32), field('topicErrors', array(topicErrorSchema))]);

const topicNameComparator = (a: { topic: string }, b: { topic: string }): number => a.topic.localeCompare(b.topic);

export const deleteTopicsResponseV1: ResponseDefinition<DeleteTopicsResponseV1Body> = {
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
