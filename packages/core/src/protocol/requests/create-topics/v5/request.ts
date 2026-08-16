import {
  boolean,
  compactArray,
  compactNullableString,
  compactString,
  defineRequest,
  field,
  flexibleObject,
  int16,
  int32,
} from '../../../schema';
import { API_KEYS } from '../../api-keys';

/**
 * CreateTopics Request (Version: 5) => [topics] timeout_ms validate_only TAG_BUFFER
 *   topics => name num_partitions replication_factor [assignments] [configs] TAG_BUFFER
 *     name => COMPACT_STRING
 *     num_partitions => INT32
 *     replication_factor => INT16
 *     assignments => partition_index [broker_ids] TAG_BUFFER
 *       partition_index => INT32
 *       broker_ids => INT32
 *     configs => name value TAG_BUFFER
 *       name => COMPACT_STRING
 *       value => COMPACT_NULLABLE_STRING
 *   timeout_ms => INT32
 *   validate_only => BOOLEAN
 *
 * First flexible version (KIP-482). Compact strings/arrays + TAG_BUFFER on every struct.
 * Request header v2's trailing TAG_BUFFER is written by `createRequest`, not here.
 * Decoded field names stay `topic` / `replicaAssignment` / `configEntries` to match this client.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const replicaAssignmentSchema = flexibleObject([field('partition', int32), field('replicas', compactArray(int32))]);
const configEntrySchema = flexibleObject([field('name', compactString), field('value', compactNullableString)]);
const topicSchema = flexibleObject([
  field('topic', compactString),
  field('numPartitions', int32),
  field('replicationFactor', int16),
  field('replicaAssignment', compactArray(replicaAssignmentSchema)),
  field('configEntries', compactArray(configEntrySchema)),
]);
export const requestSchema = flexibleObject([
  field('topics', compactArray(topicSchema)),
  field('timeout', int32),
  field('validateOnly', boolean),
]);

export function createFlexibleCreateTopicsRequest(apiVersion: 5 | 6 | 7) {
  return defineRequest({
    apiKey: API_KEYS.CreateTopics,
    apiVersion,
    apiName: 'CreateTopics',
    schema: requestSchema,
  });
}

export const createTopicsRequestV5 = createFlexibleCreateTopicsRequest(5);
