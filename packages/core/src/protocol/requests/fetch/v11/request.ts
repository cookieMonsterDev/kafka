import { ISOLATION_LEVEL } from '../../../enums/isolation-level';
import { array, defineRequest, field, int32, int64, int8, object, string } from '../../../schema';
import { API_KEYS } from '../../api-keys';
import type { FetchRequestOptions } from '../shared';

/**
 * Allow consumers to fetch from the closest replica.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-392%3A+Allow+consumers+to+fetch+from+closest+replica
 *
 * Fetch Request (Version: 11) => replica_id max_wait_time min_bytes max_bytes isolation_level session_id session_epoch [topics] [forgotten_topics_data] rack_id
 *   ...
 *   forgotten_topics_data => topic [partitions]
 *     topic => STRING
 *     partitions => INT32
 *   rack_id => STRING
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
  field('rackId', string),
]);

const baseRequest = defineRequest({
  apiKey: API_KEYS.Fetch,
  apiVersion: 11,
  apiName: 'Fetch',
  schema: requestSchema,
});

export const fetchRequestV11 = (options: FetchRequestOptions) =>
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
    rackId: options.rackId ?? '',
  });
