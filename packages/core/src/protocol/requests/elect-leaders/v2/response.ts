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
} from '../../../schema';
import { throwOnElectLeadersPartitionErrors, type ElectLeadersResponseV0Body } from '../v0/response';

export type ElectLeadersResponseV2Body = ElectLeadersResponseV0Body;

/**
 * ElectLeaders Response (Version: 2) => throttle_time_ms error_code [replica_election_results] TAG_BUFFER
 *   throttle_time_ms => INT32
 *   error_code => INT16
 *   replica_election_results => topic [partition_result] TAG_BUFFER
 *     topic => COMPACT_STRING
 *     partition_result => partition_id error_code error_message TAG_BUFFER
 *       partition_id => INT32
 *       error_code => INT16
 *       error_message => COMPACT_NULLABLE_STRING
 *
 * Flexible-version API. Response header v1's trailing TAG_BUFFER is skipped by `Connection`
 * before `decode()` runs.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const partitionSchema = flexibleObject([
  field('partition', int32),
  field('errorCode', int16),
  field('errorMessage', compactNullableString),
]);
const resultSchema = flexibleObject([
  field('topic', compactString),
  field('partitions', compactArray(partitionSchema)),
]);
export const responseSchema = flexibleObject([
  field('throttleTime', int32),
  field('errorCode', int16),
  field('results', compactArray(resultSchema)),
]);

export const electLeadersResponseV2 = defineResponse<ElectLeadersResponseV2Body>({
  schema: responseSchema,
  parse: async (data) => {
    if (failure(data.errorCode)) throw createErrorFromCode(data.errorCode);
    throwOnElectLeadersPartitionErrors(data.results);
    return data;
  },
});
