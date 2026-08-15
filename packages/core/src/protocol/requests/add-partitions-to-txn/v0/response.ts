import { createErrorFromCode, failure } from '../../../error-codes.js';
import { array, defineResponse, field, int16, int32, object, string } from '../../../schema.js';

/**
 * AddPartitionsToTxn Response (Version: 0) => throttle_time_ms [errors]
 *   throttle_time_ms => INT32
 *   errors => topic [partition_errors]
 *     topic => STRING
 *     partition_errors => partition error_code
 *       partition => INT32
 *       error_code => INT16
 */
const partitionErrorSchema = object([field('partition', int32), field('errorCode', int16)]);
const topicErrorsSchema = object([field('topic', string), field('partitionErrors', array(partitionErrorSchema))]);
const bodySchema = object([field('throttleTime', int32), field('errors', array(topicErrorsSchema))]);

export const addPartitionsToTxnResponseV0 = defineResponse({
  schema: bodySchema,
  parse: async (data) => {
    for (const { partitionErrors } of data.errors) {
      const failed = partitionErrors.find((partitionError) => failure(partitionError.errorCode));
      if (failed) throw createErrorFromCode(failed.errorCode);
    }
    return data;
  },
});
