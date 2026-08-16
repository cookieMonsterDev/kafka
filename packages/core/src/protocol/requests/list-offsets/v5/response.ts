import { array, defineResponse, field, int16, int32, int64, object, string } from '../../../schema';
import type { ResponseDefinition } from '../../../schema';
import { checkListOffsetsErrors } from '../shared';

export interface ListOffsetsResponseV5Body {
  responses: {
    topic: string;
    partitions: {
      partition: number;
      errorCode: number;
      timestamp: bigint;
      offset: bigint;
      leaderEpoch: number;
    }[];
  }[];
  throttleTime: number;
  clientSideThrottleTime: number;
}

/**
 * ListOffsets Response (Version: 5) => throttle_time_ms [responses]
 *   throttle_time_ms => INT32
 *   responses => topic [partition_responses]
 *     topic => STRING
 *     partition_responses => partition error_code timestamp offset leader_epoch
 *       partition => INT32
 *       error_code => INT16
 *       timestamp => INT64
 *       offset => INT64
 *       leader_epoch => INT32
 *
 * KIP-320: `leader_epoch` is the epoch of the record at `offset` (or -1 if unknown).
 * Throttle semantics stay the v3/KIP-219 client-side meaning.
 */
const partitionSchema = object([
  field('partition', int32),
  field('errorCode', int16),
  field('timestamp', int64),
  field('offset', int64),
  field('leaderEpoch', int32),
]);
const responseSchema = object([field('topic', string), field('partitions', array(partitionSchema))]);
const bodySchema = object([field('throttleTime', int32), field('responses', array(responseSchema))]);

const rawResponse = defineResponse({ schema: bodySchema });

export const listOffsetsResponseV5: ResponseDefinition<ListOffsetsResponseV5Body> = {
  decode: async (rawData) => {
    const decoded = await rawResponse.decode(rawData);
    return { ...decoded, throttleTime: 0, clientSideThrottleTime: decoded.throttleTime };
  },
  parse: async (data) => {
    checkListOffsetsErrors(data);
    return data;
  },
};
