import { createErrorFromCode, failure } from '../../../error-codes';
import { array, defineResponse, field, int16, int32, nullableString, object, string } from '../../../schema';
import type { ElectLeadersReplicaResult, ElectLeadersResponseV0Body } from '../v0/response';

export type ElectLeadersResponseV1Body = ElectLeadersResponseV0Body;

/**
 * ElectLeaders Response (Version: 1) => throttle_time_ms error_code [replica_election_results]
 *   throttle_time_ms => INT32
 *   error_code => INT16
 *   replica_election_results => topic [partition_result]
 *     topic => STRING
 *     partition_result => partition_id error_code error_message
 *       partition_id => INT32
 *       error_code => INT16
 *       error_message => NULLABLE_STRING
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const partitionSchema = object([
  field('partition', int32),
  field('errorCode', int16),
  field('errorMessage', nullableString),
]);
const resultSchema = object([field('topic', string), field('partitions', array(partitionSchema))]);
export const responseSchema = object([
  field('throttleTime', int32),
  field('errorCode', int16),
  field('results', array(resultSchema)),
]);

export const electLeadersResponseV1 = defineResponse<ElectLeadersResponseV1Body>({
  schema: responseSchema,
  parse: async (data) => {
    if (failure(data.errorCode)) throw createErrorFromCode(data.errorCode);
    const partitionWithError = data.results
      .flatMap((result) => result.partitions)
      .find((partition) => failure(partition.errorCode));
    if (partitionWithError) throw createErrorFromCode(partitionWithError.errorCode);
    return data;
  },
});

export type { ElectLeadersReplicaResult };
