import {
  array,
  defineResponse,
  field,
  int16,
  int32,
  int64,
  nullableString,
  object,
  string,
  type ResponseDefinition,
} from '../../../schema';
import { parseProduceResponse } from '../shared';

export interface ProduceRecordError {
  batchIndex: number;
  batchIndexErrorMessage: string | null;
}

export interface ProduceResponseV8Body {
  topics: {
    topicName: string;
    partitions: {
      partition: number;
      errorCode: number;
      baseOffset: bigint;
      logAppendTime: bigint;
      logStartOffset: bigint;
      recordErrors: ProduceRecordError[];
      errorMessage: string | null;
    }[];
  }[];
  throttleTime: number;
  clientSideThrottleTime: number;
}

/**
 * Produce Response (Version: 8) => [responses] throttle_time_ms
 *   responses => topic [partition_responses]
 *     topic => STRING
 *     partition_responses => partition error_code base_offset log_append_time log_start_offset
 *                            [record_errors] error_message
 *       partition => INT32
 *       error_code => INT16
 *       base_offset => INT64
 *       log_append_time => INT64
 *       log_start_offset => INT64
 *       record_errors => batch_index batch_index_error_message
 *         batch_index => INT32
 *         batch_index_error_message => NULLABLE_STRING
 *       error_message => NULLABLE_STRING
 *   throttle_time_ms => INT32
 *
 * Record-level errors (KIP-467). Throttle semantics match v6 (KIP-219).
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const bodySchema = object([
  field(
    'topics',
    array(
      object([
        field('topicName', string),
        field(
          'partitions',
          array(
            object([
              field('partition', int32),
              field('errorCode', int16),
              field('baseOffset', int64),
              field('logAppendTime', int64),
              field('logStartOffset', int64),
              field(
                'recordErrors',
                array(object([field('batchIndex', int32), field('batchIndexErrorMessage', nullableString)])),
              ),
              field('errorMessage', nullableString),
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

export const produceResponseV8: ResponseDefinition<ProduceResponseV8Body> = {
  decode: async (rawData) => {
    const decoded = await raw.decode(rawData);
    return { ...decoded, throttleTime: 0, clientSideThrottleTime: decoded.throttleTime };
  },
  parse: parseProduceResponse,
};
