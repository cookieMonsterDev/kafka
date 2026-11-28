import {
  compactArray,
  compactString,
  defineResponse,
  field,
  flexibleObject,
  int16,
  int32,
  int64,
} from '../../../schema';
import type { DescribeLogDirsResult } from '../v0/response';
import { topicSchema } from '../v2/response';
import { type DescribeLogDirsResponseV3Body, parseDescribeLogDirsV3 } from '../v3/response';

export interface DescribeLogDirsResultV4 extends DescribeLogDirsResult {
  /** Size of the volume holding the log dir, in bytes, or `-1n` when the broker cannot read it. */
  totalBytes: bigint;
  /** Usable space on that volume, in bytes, or `-1n` when the broker cannot read it. */
  usableBytes: bigint;
}

export interface DescribeLogDirsResponseV4Body extends DescribeLogDirsResponseV3Body {
  logDirs: DescribeLogDirsResultV4[];
}

/**
 * DescribeLogDirs Response (Version: 4) => throttle_time_ms error_code [log_dirs] TAG_BUFFER
 *   throttle_time_ms => INT32
 *   error_code => INT16
 *   log_dirs => error_code log_dir [topics] total_bytes usable_bytes TAG_BUFFER
 *     error_code => INT16
 *     log_dir => COMPACT_STRING
 *     topics => see v2
 *     total_bytes => INT64
 *     usable_bytes => INT64
 *
 * v4 adds the volume's total and usable bytes per log dir (KIP-827). Neither counts data in
 * remote storage.
 *
 * @see https://kafka.apache.org/44/design/protocol/
 */
const logDirSchema = flexibleObject([
  field('errorCode', int16),
  field('logDir', compactString),
  field('topics', compactArray(topicSchema)),
  field('totalBytes', int64),
  field('usableBytes', int64),
]);
export const responseSchema = flexibleObject([
  field('throttleTime', int32),
  field('errorCode', int16),
  field('logDirs', compactArray(logDirSchema)),
]);

export const describeLogDirsResponseV4 = defineResponse<DescribeLogDirsResponseV4Body>({
  schema: responseSchema,
  parse: parseDescribeLogDirsV3,
});
