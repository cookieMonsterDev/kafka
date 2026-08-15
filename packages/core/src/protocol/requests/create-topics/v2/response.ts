import { KafkaJSAggregateError, KafkaJSCreateTopicError } from '../../../../errors.js';
import { Decoder } from '../../../decoder.js';
import { createErrorFromCode, failure } from '../../../error-codes.js';
import type { ResponseDefinition } from '../../../schema.js';
import { array, field, int16, int32, nullableString, object, string } from '../../../schema.js';

export interface CreateTopicsResponseV2Body {
  throttleTime: number;
  topicErrors: { topic: string; errorCode: number; errorMessage: string | null }[];
}

/**
 * CreateTopics Response (Version: 2) => throttle_time_ms [topic_errors]
 *   throttle_time_ms => INT32
 *   topic_errors => topic error_code error_message
 *     topic => STRING
 *     error_code => INT16
 *     error_message => NULLABLE_STRING
 */
const topicErrorSchema = object([
  field('topic', string),
  field('errorCode', int16),
  field('errorMessage', nullableString),
]);
const bodySchema = object([field('throttleTime', int32), field('topicErrors', array(topicErrorSchema))]);

const topicNameComparator = (a: { topic: string }, b: { topic: string }): number => a.topic.localeCompare(b.topic);

export const createTopicsResponseV2: ResponseDefinition<CreateTopicsResponseV2Body> = {
  decode: async (rawData) => {
    const decoded = bodySchema.read(new Decoder(rawData));
    return { ...decoded, topicErrors: [...decoded.topicErrors].sort(topicNameComparator) };
  },
  parse: async (data) => {
    const topicsWithError = data.topicErrors.filter(({ errorCode }) => failure(errorCode));
    if (topicsWithError.length > 0) {
      throw new KafkaJSAggregateError(
        'Topic creation errors',
        topicsWithError.map((error) => new KafkaJSCreateTopicError(createErrorFromCode(error.errorCode), error.topic)),
      );
    }
    return data;
  },
};
