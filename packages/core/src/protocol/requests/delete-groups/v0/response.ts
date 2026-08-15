import { Decoder } from '../../../decoder.js';
import { createErrorFromCode, failure } from '../../../error-codes.js';
import type { KafkaJSProtocolError } from '../../../../errors.js';
import { array, field, int16, object, string } from '../../../schema.js';
import type { ResponseDefinition } from '../../../schema.js';

export interface DeleteGroupsResult {
  groupId: string;
  errorCode: number;
  error?: KafkaJSProtocolError;
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
 * kafkajs names this field `throttleTimeMs` (every other family calls it `throttleTime`) and its
 * v1 wraps this decode assuming the latter name, so v1's `clientSideThrottleTime` silently reads
 * `undefined` there — a latent, inert kafkajs bug (nothing consumes it). Normalized to
 * `throttleTime` here so v1's remap is actually correct.
 *
 * `parse` never throws: per-group failures are attached as `.error` on each result instead,
 * matching kafkajs (`admin.deleteGroups` reports failures per group, not via a single rejection).
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
