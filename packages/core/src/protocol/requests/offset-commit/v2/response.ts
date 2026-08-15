import { array, defineResponse, field, int16, int32, object, string } from '../../../schema.js';
import { checkOffsetCommitErrors } from '../shared.js';

/**
 * OffsetCommit Response (Version: 2) => [responses]
 *   responses => topic [partition_responses]
 *     topic => STRING
 *     partition_responses => partition error_code
 *       partition => INT32
 *       error_code => INT16
 */
export const partitionSchema = object([field('partition', int32), field('errorCode', int16)]);
export const responseSchema = object([field('topic', string), field('partitions', array(partitionSchema))]);
const bodySchema = object([field('responses', array(responseSchema))]);

export const offsetCommitResponseV2 = defineResponse({
  schema: bodySchema,
  parse: async (data) => {
    checkOffsetCommitErrors(data);
    return data;
  },
});
