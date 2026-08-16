import { Decoder } from '../../../decoder';
import { createErrorFromCode, failure } from '../../../error-codes';
import type { ResponseDefinition } from '../../../schema';
import { array, field, int16, object, string } from '../../../schema';

export interface DeleteTopicsResponseV0Body {
  topicErrors: { topic: string; errorCode: number }[];
}

/**
 * DeleteTopics Response (Version: 0) => [topic_error_codes]
 *   topic_error_codes => topic error_code
 *     topic => STRING
 *     error_code => INT16
 */
const topicErrorSchema = object([field('topic', string), field('errorCode', int16)]);
const bodySchema = object([field('topicErrors', array(topicErrorSchema))]);

const topicNameComparator = (a: { topic: string }, b: { topic: string }): number => a.topic.localeCompare(b.topic);

export const deleteTopicsResponseV0: ResponseDefinition<DeleteTopicsResponseV0Body> = {
  decode: async (rawData) => {
    const decoded = bodySchema.read(new Decoder(rawData));
    return { topicErrors: [...decoded.topicErrors].sort(topicNameComparator) };
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
