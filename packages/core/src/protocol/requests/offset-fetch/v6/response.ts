import { createErrorFromCode, failure } from '../../../error-codes';
import {
  compactArray,
  compactNullableString,
  compactString,
  defineResponse,
  field,
  flexibleObject,
  int16,
  int32,
  int64,
} from '../../../schema';
import type { ResponseDefinition } from '../../../schema';
import { checkOffsetFetchPartitionErrors } from '../shared';
import type { OffsetFetchResponseV5Body } from '../v5/response';

export type OffsetFetchResponseV6Body = OffsetFetchResponseV5Body;

/**
 * OffsetFetch Response (Version: 6) => throttle_time_ms [responses] error_code TAG_BUFFER
 *   throttle_time_ms => INT32
 *   responses => topic [partition_responses] TAG_BUFFER
 *     topic => COMPACT_STRING
 *     partition_responses => partition offset committed_leader_epoch metadata error_code TAG_BUFFER
 *       partition => INT32
 *       offset => INT64
 *       committed_leader_epoch => INT32
 *       metadata => COMPACT_NULLABLE_STRING
 *       error_code => INT16
 *   error_code => INT16
 *
 * First flexible version of v5 (KIP-482). Response header v1's trailing TAG_BUFFER is skipped
 * by `Connection` before `decode()` runs. Quota timing follows v4 (KIP-219).
 */
const partitionSchema = flexibleObject([
  field('partition', int32),
  field('offset', int64),
  field('leaderEpoch', int32),
  field('metadata', compactNullableString),
  field('errorCode', int16),
]);
const responseSchema = flexibleObject([
  field('topic', compactString),
  field('partitions', compactArray(partitionSchema)),
]);
const bodySchema = flexibleObject([
  field('throttleTime', int32),
  field('responses', compactArray(responseSchema)),
  field('errorCode', int16),
]);

const rawResponse = defineResponse({ schema: bodySchema });

export const offsetFetchResponseV6: ResponseDefinition<OffsetFetchResponseV6Body> = {
  decode: async (rawData) => {
    const decoded = await rawResponse.decode(rawData);
    return { ...decoded, throttleTime: 0, clientSideThrottleTime: decoded.throttleTime };
  },
  parse: async (data) => {
    if (failure(data.errorCode)) throw createErrorFromCode(data.errorCode);
    checkOffsetFetchPartitionErrors(data);
    return data;
  },
};
