import { ISOLATION_LEVEL } from '../../../enums/isolation-level';
import { array, defineRequest, field, int32, int64, int8, object, string } from '../../../schema';
import { API_KEYS } from '../../api-keys';
import type { FetchRequestOptions } from '../shared';

/**
 * Allow fetchers to detect and handle log truncation.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-320%3A+Allow+fetchers+to+detect+and+handle+log+truncation
 *
 * Fetch Request (Version: 9) => replica_id max_wait_time min_bytes max_bytes isolation_level session_id session_epoch [topics] [forgotten_topics_data]
 *   ...
 *   topics => topic [partitions]
 *     topic => STRING
 *     partitions => partition current_leader_epoch fetch_offset log_start_offset partition_max_bytes
 *       partition => INT32
 *       current_leader_epoch => INT32
 *       fetch_offset => INT64
 *       log_start_offset => INT64
 *       partition_max_bytes => INT32
 *   forgotten_topics_data => topic [partitions]
 *     topic => STRING
 *     partitions => INT32
 */
const requestSchema = object([
  field('replicaId', int32),
  field('maxWaitTime', int32),
  field('minBytes', int32),
  field('maxBytes', int32),
  field('isolationLevel', int8),
  field('sessionId', int32),
  field('sessionEpoch', int32),
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
              field('currentLeaderEpoch', int32),
              field('fetchOffset', int64),
              field('logStartOffset', int64),
              field('maxBytes', int32),
            ]),
          ),
        ),
      ]),
    ),
  ),
  field('forgottenTopics', array(object([field('topic', string), field('partitions', array(int32))]))),
]);

const baseRequest = defineRequest({
  apiKey: API_KEYS.Fetch,
  apiVersion: 9,
  apiName: 'Fetch',
  schema: requestSchema,
});

export const fetchRequestV9 = (options: FetchRequestOptions) =>
  baseRequest({
    replicaId: options.replicaId,
    maxWaitTime: options.maxWaitTime,
    minBytes: options.minBytes,
    maxBytes: options.maxBytes,
    isolationLevel: options.isolationLevel ?? ISOLATION_LEVEL.READ_COMMITTED,
    sessionId: options.sessionId ?? 0,
    sessionEpoch: options.sessionEpoch ?? -1,
    topics: options.topics.map(({ topic, partitions }) => ({
      topic,
      partitions: partitions.map(({ partition, currentLeaderEpoch, fetchOffset, logStartOffset, maxBytes }) => ({
        partition,
        currentLeaderEpoch: currentLeaderEpoch ?? -1,
        fetchOffset,
        logStartOffset: logStartOffset ?? -1n,
        maxBytes,
      })),
    })),
    forgottenTopics: (options.forgottenTopics ?? []).map(({ topic, partitions }) => ({ topic, partitions })),
  });
