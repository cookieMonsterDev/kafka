import {
  compactArray,
  compactNullableString,
  defineRequest,
  field,
  flexibleObject,
  int32,
  uuid,
} from '../../../schema';
import { API_KEYS } from '../../api-keys';

const ZERO_UUID = Buffer.alloc(16);

/**
 * DeleteTopics Request (Version: 6) => [topics] timeout_ms TAG_BUFFER
 *   topics => name topic_id TAG_BUFFER
 *     name => COMPACT_NULLABLE_STRING
 *     topic_id => UUID
 *   timeout_ms => INT32
 *
 * Topics become `{ name, topicId }` structs (KIP-516). Name-only deletes write the topic name
 * and a zero UUID. Request header v2's trailing TAG_BUFFER is written by `createRequest`, not here.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const topicSchema = flexibleObject([field('name', compactNullableString), field('topicId', uuid)]);
export const requestSchema = flexibleObject([field('topics', compactArray(topicSchema)), field('timeout', int32)]);

const create = defineRequest({
  apiKey: API_KEYS.DeleteTopics,
  apiVersion: 6,
  apiName: 'DeleteTopics',
  schema: requestSchema,
});

export const deleteTopicsRequestV6 = ({ topics, timeout }: { topics: string[]; timeout: number }) =>
  create({
    topics: topics.map((name) => ({ name, topicId: ZERO_UUID })),
    timeout,
  });
