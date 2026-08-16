import { compactArray, compactString, defineRequest, field, flexibleObject, int32 } from '../../../schema';
import { API_KEYS } from '../../api-keys';

/**
 * DeleteTopics Request (Version: 4) => [topic_names] timeout_ms TAG_BUFFER
 *   topic_names => COMPACT_STRING
 *   timeout_ms => INT32
 *
 * First flexible version (KIP-482). Compact array of compact topic name strings + timeout +
 * TAG_BUFFER. Request header v2's trailing TAG_BUFFER is written by `createRequest`, not here.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export const requestSchema = flexibleObject([field('topics', compactArray(compactString)), field('timeout', int32)]);

export function createFlexibleDeleteTopicsRequest(apiVersion: 4 | 5) {
  return defineRequest({
    apiKey: API_KEYS.DeleteTopics,
    apiVersion,
    apiName: 'DeleteTopics',
    schema: requestSchema,
  });
}

export const deleteTopicsRequestV4 = createFlexibleDeleteTopicsRequest(4);
