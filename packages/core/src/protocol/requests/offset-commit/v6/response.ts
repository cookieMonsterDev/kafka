import type { ResponseDefinition } from '../../../schema';
import type { OffsetCommitResponseV4Body } from '../v4/response';
import { offsetCommitResponseV5 } from '../v5/response';

export type OffsetCommitResponseV6Body = OffsetCommitResponseV4Body;

/**
 * OffsetCommit Response (Version: 6) => throttle_time_ms [responses]
 *   throttle_time_ms => INT32
 *   responses => topic [partition_responses]
 *     topic => STRING
 *     partition_responses => partition error_code
 *       partition => INT32
 *       error_code => INT16
 *
 * Wire format is identical to v5.
 */
export const offsetCommitResponseV6: ResponseDefinition<OffsetCommitResponseV6Body> = {
  decode: (rawData) => offsetCommitResponseV5.decode(rawData),
  parse: (data) => offsetCommitResponseV5.parse(data),
};
