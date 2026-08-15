import { array, defineResponse, field, int16, int32, int64, object, string } from '../../../schema.js';
import { parseProduceResponse } from '../shared.js';

/**
 * Produce Response (Version: 5) => [responses] throttle_time_ms
 *   responses => topic [partition_responses]
 *     topic => STRING
 *     partition_responses => partition error_code base_offset log_append_time log_start_offset
 *       partition => INT32
 *       error_code => INT16
 *       base_offset => INT64
 *       log_append_time => INT64
 *       log_start_offset => INT64
 *   throttle_time_ms => INT32
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
            ]),
          ),
        ),
      ]),
    ),
  ),
  field('throttleTime', int32),
]);

export const produceResponseV5 = defineResponse({
  schema: bodySchema,
  parse: parseProduceResponse,
});
