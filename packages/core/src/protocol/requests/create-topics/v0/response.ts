import { KafkaAggregateError, KafkaCreateTopicError } from '../../../../errors';
import { Decoder } from '../../../decoder';
import { createErrorFromCode, failure } from '../../../error-codes';
import type { ResponseDefinition } from '../../../schema';
import { array, field, int16, object, string } from '../../../schema';

export interface CreateTopicsResponseV0Body {
  topicErrors: { topic: string; errorCode: number }[];
}

/**
 * CreateTopics Response (Version: 0) => [topic_errors]
 *   topic_errors => topic error_code
 *     topic => STRING
 *     error_code => INT16
 */
const topicErrorSchema = object([field('topic', string), field('errorCode', int16)]);
const bodySchema = object([field('topicErrors', array(topicErrorSchema))]);

const topicNameComparator = (a: { topic: string }, b: { topic: string }): number => a.topic.localeCompare(b.topic);

export function parseCreateTopicsErrors<T extends { topicErrors: { topic: string; errorCode: number }[] }>(data: T): T {
  const topicsWithError = data.topicErrors.filter(({ errorCode }) => failure(errorCode));
  if (topicsWithError.length > 0) {
    throw new KafkaAggregateError(
      'Topic creation errors',
      topicsWithError.map((error) => new KafkaCreateTopicError(createErrorFromCode(error.errorCode), error.topic)),
    );
  }
  return data;
}

export const createTopicsResponseV0: ResponseDefinition<CreateTopicsResponseV0Body> = {
  decode: async (rawData) => {
    const decoded = bodySchema.read(new Decoder(rawData));
    return { topicErrors: [...decoded.topicErrors].sort(topicNameComparator) };
  },
  parse: async (data) => parseCreateTopicsErrors(data),
};
