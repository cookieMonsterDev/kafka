import { array, defineResponse, field, int16, int32, int64, object, string } from '../../../schema';
import { checkListOffsetsErrors } from '../shared';

/**
 * ListOffsets Response (Version: 2) => throttle_time_ms [responses]
 *   throttle_time_ms => INT32
 *   responses => topic [partition_responses]
 *     topic => STRING
 *     partition_responses => partition error_code timestamp offset
 *       partition => INT32
 *       error_code => INT16
 *       timestamp => INT64
 *       offset => INT64
 */
const partitionSchema = object([
  field('partition', int32),
  field('errorCode', int16),
  field('timestamp', int64),
  field('offset', int64),
]);
const responseSchema = object([field('topic', string), field('partitions', array(partitionSchema))]);
const bodySchema = object([field('throttleTime', int32), field('responses', array(responseSchema))]);

export const listOffsetsResponseV2 = defineResponse({
  schema: bodySchema,
  parse: async (data) => {
    checkListOffsetsErrors(data);
    return data;
  },
});
