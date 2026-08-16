import { Decoder } from '../../../decoder';
import type { ResponseDefinition } from '../../../schema';
import { array, field, int16, nullableString, object, string } from '../../../schema';
import { parseCreateTopicsErrors } from '../v0/response';

export interface CreateTopicsResponseV1Body {
  topicErrors: { topic: string; errorCode: number; errorMessage: string | null }[];
}

/**
 * CreateTopics Response (Version: 1) => [topic_errors]
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
const bodySchema = object([field('topicErrors', array(topicErrorSchema))]);

const topicNameComparator = (a: { topic: string }, b: { topic: string }): number => a.topic.localeCompare(b.topic);

export const createTopicsResponseV1: ResponseDefinition<CreateTopicsResponseV1Body> = {
  decode: async (rawData) => {
    const decoded = bodySchema.read(new Decoder(rawData));
    return { topicErrors: [...decoded.topicErrors].sort(topicNameComparator) };
  },
  parse: async (data) => parseCreateTopicsErrors(data),
};
