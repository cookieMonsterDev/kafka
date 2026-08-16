import type { ResponseDefinition } from '../../../schema';
import { offsetCommitResponseV6, type OffsetCommitResponseV6Body } from '../v6/response';

export type OffsetCommitResponseV7Body = OffsetCommitResponseV6Body;

/**
 * OffsetCommit Response (Version: 7) => throttle_time_ms [responses]
 *
 * Wire format is identical to v6. Request v7 adds group_instance_id; the response shape is unchanged.
 */
export const offsetCommitResponseV7: ResponseDefinition<OffsetCommitResponseV7Body> = {
  decode: (rawData) => offsetCommitResponseV6.decode(rawData),
  parse: (data) => offsetCommitResponseV6.parse(data),
};
