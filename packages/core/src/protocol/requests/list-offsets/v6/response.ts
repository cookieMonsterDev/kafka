import {
  compactArray,
  compactString,
  defineResponse,
  field,
  flexibleObject,
  int16,
  int32,
  int64,
} from '../../../schema';
import type { ResponseDefinition } from '../../../schema';
import type { ListOffsetsResponseV5Body } from '../v5/response';
import { checkListOffsetsErrors } from '../shared';

export type ListOffsetsResponseV6Body = ListOffsetsResponseV5Body;

/**
 * ListOffsets Response (Version: 6) => throttle_time_ms [responses] TAG_BUFFER
 *   throttle_time_ms => INT32
 *   responses => topic [partition_responses] TAG_BUFFER
 *     topic => COMPACT_STRING
 *     partition_responses => partition error_code timestamp offset leader_epoch TAG_BUFFER
 *       partition => INT32
 *       error_code => INT16
 *       timestamp => INT64
 *       offset => INT64
 *       leader_epoch => INT32
 *
 * Flexible v5 (compact + tagged). Response header v1's trailing TAG_BUFFER is skipped by
 * `Connection` before `decode()` runs. Throttle semantics stay the v3/KIP-219 client-side meaning.
 */
const bodySchema = flexibleObject([
  field('throttleTime', int32),
  field(
    'responses',
    compactArray(
      flexibleObject([
        field('topic', compactString),
        field(
          'partitions',
          compactArray(
            flexibleObject([
              field('partition', int32),
              field('errorCode', int16),
              field('timestamp', int64),
              field('offset', int64),
              field('leaderEpoch', int32),
            ]),
          ),
        ),
      ]),
    ),
  ),
]);

const rawResponse = defineResponse({ schema: bodySchema });

export const listOffsetsResponseV6: ResponseDefinition<ListOffsetsResponseV6Body> = {
  decode: async (rawData) => {
    const decoded = await rawResponse.decode(rawData);
    return { ...decoded, throttleTime: 0, clientSideThrottleTime: decoded.throttleTime };
  },
  parse: async (data) => {
    checkListOffsetsErrors(data);
    return data;
  },
};
