import { ISOLATION_LEVEL } from '../../../enums/isolation-level';
import { array, defineRequest, field, int32, int64, int8, object, string } from '../../../schema';
import { API_KEYS } from '../../api-keys';
import type { FetchRequestOptions } from '../shared';

/**
 * Fetch Request (Version: 5) => replica_id max_wait_time min_bytes max_bytes isolation_level [topics]
 *   replica_id => INT32
 *   max_wait_time => INT32
 *   min_bytes => INT32
 *   max_bytes => INT32
 *   isolation_level => INT8
 *   topics => topic [partitions]
 *     topic => STRING
 *     partitions => partition fetch_offset log_start_offset partition_max_bytes
 *       partition => INT32
 *       fetch_offset => INT64
 *       log_start_offset => INT64
 *       partition_max_bytes => INT32
 */
const requestSchema = object([
  field('replicaId', int32),
  field('maxWaitTime', int32),
  field('minBytes', int32),
  field('maxBytes', int32),
  field('isolationLevel', int8),
  field(
    'topics',
    array(
      object([
        field('topic', string),
        field(
          'partitions',
          array(
            object([
              field('partition', int32),
              field('fetchOffset', int64),
              field('logStartOffset', int64),
              field('maxBytes', int32),
            ]),
          ),
        ),
      ]),
    ),
  ),
]);

const baseRequest = defineRequest({
  apiKey: API_KEYS.Fetch,
  apiVersion: 5,
  apiName: 'Fetch',
  schema: requestSchema,
});

export const fetchRequestV5 = (options: FetchRequestOptions) =>
  baseRequest({
    replicaId: options.replicaId,
    maxWaitTime: options.maxWaitTime,
    minBytes: options.minBytes,
    maxBytes: options.maxBytes,
    isolationLevel: options.isolationLevel ?? ISOLATION_LEVEL.READ_COMMITTED,
    topics: options.topics.map(({ topic, partitions }) => ({
      topic,
      partitions: partitions.map(({ partition, fetchOffset, logStartOffset, maxBytes }) => ({
        partition,
        fetchOffset,
        logStartOffset: logStartOffset ?? -1n,
        maxBytes,
      })),
    })),
  });
