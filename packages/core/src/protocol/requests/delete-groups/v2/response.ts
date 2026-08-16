import { Decoder } from '../../../decoder';
import { createErrorFromCode, failure } from '../../../error-codes';
import { compactArray, compactString, field, flexibleObject, int16, int32 } from '../../../schema';
import type { ResponseDefinition } from '../../../schema';
import type { DeleteGroupsResult } from '../v0/response';

export interface DeleteGroupsResponseV2Body {
  results: DeleteGroupsResult[];
  throttleTime: number;
  clientSideThrottleTime: number;
}

/**
 * DeleteGroups Response (Version: 2) => throttle_time_ms [results] TAG_BUFFER
 *   throttle_time_ms => INT32
 *   results => group_id error_code TAG_BUFFER
 *     group_id => COMPACT_STRING
 *     error_code => INT16
 *
 * First flexible version (KIP-482). `parse` never throws: per-group failures are attached as
 * `.error` on each result, matching v0. Quota timing follows v1 (KIP-219).
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const resultSchema = flexibleObject([field('groupId', compactString), field('errorCode', int16)]);
const bodySchema = flexibleObject([field('throttleTime', int32), field('results', compactArray(resultSchema))]);

export const deleteGroupsResponseV2: ResponseDefinition<DeleteGroupsResponseV2Body> = {
  decode: async (rawData) => {
    const decoded = bodySchema.read(new Decoder(rawData));
    const results = decoded.results.map((result) =>
      failure(result.errorCode) ? { ...result, error: createErrorFromCode(result.errorCode) } : result,
    );
    return { throttleTime: 0, clientSideThrottleTime: decoded.throttleTime, results };
  },
  parse: async (data) => data,
};
