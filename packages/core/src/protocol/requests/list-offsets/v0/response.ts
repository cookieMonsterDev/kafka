import { array, defineResponse, field, int16, int32, int64, object, string } from '../../../schema';
import { checkListOffsetsErrors } from '../shared';

export interface ListOffsetsResponseV0Body {
  responses: {
    topic: string;
    partitions: { partition: number; errorCode: number; offsets: bigint[] }[];
  }[];
}

/**
 * Offsets Response (Version: 0) => [responses]
 *   responses => topic [partition_responses]
 *     topic => STRING
 *     partition_responses => partition error_code [offsets]
 *       partition => INT32
 *       error_code => INT16
 *       offsets => INT64
 */
const partitionSchema = object([field('partition', int32), field('errorCode', int16), field('offsets', array(int64))]);
const responseSchema = object([field('topic', string), field('partitions', array(partitionSchema))]);
const bodySchema = object([field('responses', array(responseSchema))]);

export const listOffsetsResponseV0 = defineResponse({
  schema: bodySchema,
  parse: async (data) => {
    checkListOffsetsErrors(data);
    return data;
  },
});
