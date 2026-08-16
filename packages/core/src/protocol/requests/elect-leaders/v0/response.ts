import { Decoder } from '../../../decoder';
import { createErrorFromCode, failure } from '../../../error-codes';
import { array, field, int16, int32, nullableString, object, string } from '../../../schema';
import type { ResponseDefinition } from '../../../schema';

export interface ElectLeadersPartitionResult {
  partition: number;
  errorCode: number;
  errorMessage: string | null;
}

export interface ElectLeadersReplicaResult {
  topic: string;
  partitions: ElectLeadersPartitionResult[];
}

export interface ElectLeadersResponseV0Body {
  throttleTime: number;
  errorCode: number;
  results: ElectLeadersReplicaResult[];
}

/**
 * ElectLeaders Response (Version: 0) => throttle_time_ms [replica_election_results]
 *   throttle_time_ms => INT32
 *   replica_election_results => topic [partition_result]
 *     topic => STRING
 *     partition_result => partition_id error_code error_message
 *       partition_id => INT32
 *       error_code => INT16
 *       error_message => NULLABLE_STRING
 *
 * v0 has no top-level error_code; decode synthesizes `errorCode: 0` so callers share a body shape
 * with v1+.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const partitionSchema = object([
  field('partition', int32),
  field('errorCode', int16),
  field('errorMessage', nullableString),
]);
const resultSchema = object([field('topic', string), field('partitions', array(partitionSchema))]);
export const responseSchema = object([field('throttleTime', int32), field('results', array(resultSchema))]);

function throwOnPartitionErrors(results: ElectLeadersReplicaResult[]): void {
  const partitionWithError = results
    .flatMap((result) => result.partitions)
    .find((partition) => failure(partition.errorCode));
  if (partitionWithError) throw createErrorFromCode(partitionWithError.errorCode);
}

export const electLeadersResponseV0: ResponseDefinition<ElectLeadersResponseV0Body> = {
  decode: async (rawData) => {
    const data = responseSchema.read(new Decoder(rawData));
    return { ...data, errorCode: 0 };
  },
  parse: async (data) => {
    throwOnPartitionErrors(data.results);
    return data;
  },
};
