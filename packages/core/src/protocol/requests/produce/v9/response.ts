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
  type ResponseDefinition,
} from '../../../schema';
import { parseProduceResponse } from '../shared';
import type { ProduceResponseV8Body } from '../v8/response';

export type ProduceResponseV9Body = ProduceResponseV8Body;

/**
 * Produce Response (Version: 9) => [responses] throttle_time_ms TAG_BUFFER
 *   responses => topic [partition_responses] TAG_BUFFER
 *     topic => COMPACT_STRING
 *     partition_responses => partition error_code base_offset log_append_time log_start_offset
 *                            [record_errors] error_message TAG_BUFFER
 *       partition => INT32
 *       error_code => INT16
 *       base_offset => INT64
 *       log_append_time => INT64
 *       log_start_offset => INT64
 *       record_errors => batch_index batch_index_error_message TAG_BUFFER
 *         batch_index => INT32
 *         batch_index_error_message => COMPACT_NULLABLE_STRING
 *       error_message => COMPACT_NULLABLE_STRING
 *   throttle_time_ms => INT32
 *
 * Flexible version of v8 (KIP-482). throttle_time_ms stays INT32.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const bodySchema = flexibleObject([
  field(
    'topics',
    compactArray(
      flexibleObject([
        field('topicName', compactString),
        field(
          'partitions',
          compactArray(
            flexibleObject([
              field('partition', int32),
              field('errorCode', int16),
              field('baseOffset', int64),
              field('logAppendTime', int64),
              field('logStartOffset', int64),
              field(
                'recordErrors',
                compactArray(
                  flexibleObject([field('batchIndex', int32), field('batchIndexErrorMessage', compactNullableString)]),
                ),
              ),
              field('errorMessage', compactNullableString),
            ]),
          ),
        ),
      ]),
    ),
  ),
  field('throttleTime', int32),
]);

const raw = defineResponse({
  schema: bodySchema,
  parse: parseProduceResponse,
});

export const produceResponseV9: ResponseDefinition<ProduceResponseV9Body> = {
  decode: async (rawData) => {
    const decoded = await raw.decode(rawData);
    return { ...decoded, throttleTime: 0, clientSideThrottleTime: decoded.throttleTime };
  },
  parse: parseProduceResponse,
};
