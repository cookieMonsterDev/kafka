import { array, defineRequest, field, int32, int64, object, string } from '../../../schema';
import { API_KEYS } from '../../api-keys';
import type { FetchRequestOptions } from '../shared';

/**
 * Fetch Request (Version: 3) => replica_id max_wait_time min_bytes max_bytes [topics]
 *   replica_id => INT32
 *   max_wait_time => INT32
 *   min_bytes => INT32
 *   max_bytes => INT32
 *   topics => topic [partitions]
 *     topic => STRING
 *     partitions => partition fetch_offset max_bytes
 *       partition => INT32
 *       fetch_offset => INT64
 *       max_bytes => INT32
 */
const requestSchema = object([
  field('replicaId', int32),
  field('maxWaitTime', int32),
  field('minBytes', int32),
  field('maxBytes', int32),
  field(
    'topics',
    array(
      object([
        field('topic', string),
        field(
          'partitions',
          array(object([field('partition', int32), field('fetchOffset', int64), field('maxBytes', int32)])),
        ),
      ]),
    ),
  ),
]);

const baseRequest = defineRequest({
  apiKey: API_KEYS.Fetch,
  apiVersion: 3,
  apiName: 'Fetch',
  schema: requestSchema,
});

export const fetchRequestV3 = (options: FetchRequestOptions) =>
  baseRequest({
    replicaId: options.replicaId,
    maxWaitTime: options.maxWaitTime,
    minBytes: options.minBytes,
    maxBytes: options.maxBytes,
    topics: options.topics.map(({ topic, partitions }) => ({
      topic,
      partitions: partitions.map(({ partition, fetchOffset, maxBytes }) => ({
        partition,
        fetchOffset,
        maxBytes,
      })),
    })),
  });
