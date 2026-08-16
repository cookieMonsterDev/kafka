import type { ResponseDefinition } from '../../../schema';
import { offsetCommitResponseV4, type OffsetCommitResponseV4Body } from '../v4/response';

/**
 * OffsetCommit Response (Version: 5) => throttle_time_ms [responses]
 *   throttle_time_ms => INT32
 *   responses => topic [partition_responses]
 *     topic => STRING
 *     partition_responses => partition error_code
 *       partition => INT32
 *       error_code => INT16
 *
 * Wire format is identical to v4.
 */
export const offsetCommitResponseV5: ResponseDefinition<OffsetCommitResponseV4Body> = {
  decode: (rawData) => offsetCommitResponseV4.decode(rawData),
  parse: (data) => offsetCommitResponseV4.parse(data),
};
