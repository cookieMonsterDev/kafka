import { createErrorFromCode, failure } from '../../../error-codes';
import { array, defineResponse, field, int16, int32, int64, nullableString, object, string } from '../../../schema';
import type { ResponseDefinition } from '../../../schema';
import { checkOffsetFetchPartitionErrors } from '../shared';

export interface OffsetFetchResponseV5Body {
  responses: {
    topic: string;
    partitions: {
      partition: number;
      offset: bigint;
      leaderEpoch: number;
      metadata: string | null;
      errorCode: number;
    }[];
  }[];
  errorCode: number;
  throttleTime: number;
  clientSideThrottleTime: number;
}

/**
 * OffsetFetch Response (Version: 5) => throttle_time_ms [responses] error_code
 *   throttle_time_ms => INT32
 *   responses => topic [partition_responses]
 *     topic => STRING
 *     partition_responses => partition offset committed_leader_epoch metadata error_code
 *       partition => INT32
 *       offset => INT64
 *       committed_leader_epoch => INT32
 *       metadata => NULLABLE_STRING
 *       error_code => INT16
 *   error_code => INT16
 *
 * Version 5 adds the leader epoch of the committed offset (KIP-320). Throttle semantics stay
 * the v4/KIP-219 client-side meaning.
 */
const partitionSchema = object([
  field('partition', int32),
  field('offset', int64),
  field('leaderEpoch', int32),
  field('metadata', nullableString),
  field('errorCode', int16),
]);
const responseSchema = object([field('topic', string), field('partitions', array(partitionSchema))]);
const bodySchema = object([
  field('throttleTime', int32),
  field('responses', array(responseSchema)),
  field('errorCode', int16),
]);

const rawResponse = defineResponse({ schema: bodySchema });

export const offsetFetchResponseV5: ResponseDefinition<OffsetFetchResponseV5Body> = {
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
