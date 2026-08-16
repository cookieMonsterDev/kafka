import { compactArray, compactString, defineResponse, field, flexibleObject, int16, int32 } from '../../../schema';
import type { ResponseDefinition } from '../../../schema';
import { checkOffsetCommitErrors } from '../shared';
import type { OffsetCommitResponseV4Body } from '../v4/response';

export type OffsetCommitResponseV8Body = OffsetCommitResponseV4Body;

/**
 * OffsetCommit Response (Version: 8) => throttle_time_ms [responses] TAG_BUFFER
 *   throttle_time_ms => INT32
 *   responses => topic [partition_responses] TAG_BUFFER
 *     topic => COMPACT_STRING
 *     partition_responses => partition error_code TAG_BUFFER
 *       partition => INT32
 *       error_code => INT16
 *
 * First flexible version (KIP-482). Same fields as v7; compact types + TAG_BUFFER on every struct.
 * Response header v1's trailing TAG_BUFFER is skipped by `Connection` before `decode()` runs.
 * Quota timing follows v4 (KIP-219).
 */
const partitionSchema = flexibleObject([field('partition', int32), field('errorCode', int16)]);
const topicSchema = flexibleObject([field('topic', compactString), field('partitions', compactArray(partitionSchema))]);
const bodySchema = flexibleObject([field('throttleTime', int32), field('responses', compactArray(topicSchema))]);

const rawResponse = defineResponse({ schema: bodySchema });

export const offsetCommitResponseV8: ResponseDefinition<OffsetCommitResponseV8Body> = {
  decode: async (rawData) => {
    const decoded = await rawResponse.decode(rawData);
    return { ...decoded, throttleTime: 0, clientSideThrottleTime: decoded.throttleTime };
  },
  parse: async (data) => {
    checkOffsetCommitErrors(data);
    return data;
  },
};
