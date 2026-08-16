import { ISOLATION_LEVEL } from '../../../enums/isolation-level';
import { compactArray, compactString, defineRequest, field, flexibleObject, int32, int64, int8 } from '../../../schema';
import { API_KEYS } from '../../api-keys';
import type { FetchRequestOptions } from '../shared';

/**
 * Fetch Request (Version: 12) => replica_id max_wait_ms min_bytes max_bytes isolation_level
 *                                session_id session_epoch [topics] [forgotten_topics_data] rack_id TAG_BUFFER
 *   replica_id => INT32
 *   max_wait_ms => INT32
 *   min_bytes => INT32
 *   max_bytes => INT32
 *   isolation_level => INT8
 *   session_id => INT32
 *   session_epoch => INT32
 *   topics => topic [partitions] TAG_BUFFER
 *     topic => COMPACT_STRING
 *     partitions => partition current_leader_epoch fetch_offset last_fetched_epoch
 *                   log_start_offset partition_max_bytes TAG_BUFFER
 *       partition => INT32
 *       current_leader_epoch => INT32
 *       fetch_offset => INT64
 *       last_fetched_epoch => INT32
 *       log_start_offset => INT64
 *       partition_max_bytes => INT32
 *   forgotten_topics_data => topic [partitions] TAG_BUFFER
 *     topic => COMPACT_STRING
 *     partitions => INT32
 *   rack_id => COMPACT_STRING
 *
 * First flexible Fetch version (KIP-482). Adds last_fetched_epoch for KIP-320 truncation
 * detection. ClusterId is tagged field 0 and omitted (empty TAG_BUFFER). Topic names remain
 * through v12; topic IDs start at v13.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const partitionSchema = flexibleObject([
  field('partition', int32),
  field('currentLeaderEpoch', int32),
  field('fetchOffset', int64),
  field('lastFetchedEpoch', int32),
  field('logStartOffset', int64),
  field('maxBytes', int32),
]);
const topicSchema = flexibleObject([field('topic', compactString), field('partitions', compactArray(partitionSchema))]);
const forgottenTopicSchema = flexibleObject([field('topic', compactString), field('partitions', compactArray(int32))]);
export const requestSchema = flexibleObject([
  field('replicaId', int32),
  field('maxWaitTime', int32),
  field('minBytes', int32),
  field('maxBytes', int32),
  field('isolationLevel', int8),
  field('sessionId', int32),
  field('sessionEpoch', int32),
  field('topics', compactArray(topicSchema)),
  field('forgottenTopics', compactArray(forgottenTopicSchema)),
  field('rackId', compactString),
]);

const baseRequest = defineRequest({
  apiKey: API_KEYS.Fetch,
  apiVersion: 12,
  apiName: 'Fetch',
  schema: requestSchema,
});

export const fetchRequestV12 = (options: FetchRequestOptions) =>
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
      partitions: partitions.map(
        ({ partition, currentLeaderEpoch, fetchOffset, lastFetchedEpoch, logStartOffset, maxBytes }) => ({
          partition,
          currentLeaderEpoch: currentLeaderEpoch ?? -1,
          fetchOffset,
          lastFetchedEpoch: lastFetchedEpoch ?? -1,
          logStartOffset: logStartOffset ?? -1n,
          maxBytes,
        }),
      ),
    })),
    forgottenTopics: (options.forgottenTopics ?? []).map(({ topic, partitions }) => ({ topic, partitions })),
    rackId: options.rackId ?? '',
  });
