import { Decoder } from '../../../decoder';
import { createErrorFromCode, failure } from '../../../error-codes';
import type { KafkaProtocolError } from '../../../../errors';
import { array, field, int16, object, string } from '../../../schema';
import type { ResponseDefinition } from '../../../schema';

export interface DeleteGroupsResult {
  groupId: string;
  errorCode: number;
  error?: KafkaProtocolError;
}

export interface DeleteGroupsResponseV0Body {
  throttleTime: number;
  results: DeleteGroupsResult[];
}

const resultSchema = object([field('groupId', string), field('errorCode', int16)]);
const resultsSchema = array(resultSchema);

/**
 * DeleteGroups Response (Version: 0) => throttle_time_ms [results]
 *   throttle_time_ms => INT32
 *   results => group_id error_code
 *     group_id => STRING
 *     error_code => INT16
 *
 * `parse` never throws: per-group failures are attached as `.error` on each result.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export const deleteGroupsResponseV0: ResponseDefinition<DeleteGroupsResponseV0Body> = {
  decode: async (rawData) => {
    const decoder = new Decoder(rawData);
    const throttleTime = decoder.readInt32();
    const results = resultsSchema
      .read(decoder)
      .map((result) =>
        failure(result.errorCode) ? { ...result, error: createErrorFromCode(result.errorCode) } : result,
      );

    return { throttleTime, results };
  },
  parse: async (data) => data,
};
