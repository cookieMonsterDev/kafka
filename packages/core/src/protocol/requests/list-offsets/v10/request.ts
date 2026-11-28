import { defineRequest, field, flexibleObject, int8, int32 } from '../../../schema';
import { API_KEYS } from '../../api-keys';
import { flexibleTopicsField } from '../v6/request';

/**
 * ListOffsets Request (Version: 10) => replica_id isolation_level [topics] timeout_ms TAG_BUFFER
 *   replica_id => INT32
 *   isolation_level => INT8
 *   topics => see v6
 *   timeout_ms => INT32
 *
 * Adds `timeout_ms` (KIP-1075): how long the broker may wait for a lookup that reads tiered storage
 * before it answers that partition with `REQUEST_TIMED_OUT`.
 */
const requestSchema = flexibleObject([
  field('replicaId', int32),
  field('isolationLevel', int8),
  flexibleTopicsField,
  field('timeoutMs', int32),
]);

export function createListOffsetsRequestWithTimeout(apiVersion: 10 | 11) {
  return defineRequest({
    apiKey: API_KEYS.ListOffsets,
    apiVersion,
    apiName: 'ListOffsets',
    schema: requestSchema,
  });
}

export const listOffsetsRequestV10 = createListOffsetsRequestWithTimeout(10);
