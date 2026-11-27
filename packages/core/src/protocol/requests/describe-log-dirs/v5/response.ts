import {
  boolean,
  compactArray,
  compactString,
  defineResponse,
  field,
  flexibleObject,
  int16,
  int32,
  int64,
} from '../../../schema';
import { topicSchema } from '../v2/response';
import { parseDescribeLogDirsV3 } from '../v3/response';
import type { DescribeLogDirsResponseV4Body, DescribeLogDirsResultV4 } from '../v4/response';

export interface DescribeLogDirsResultV5 extends DescribeLogDirsResultV4 {
  /** True when the broker places no new partition replicas on this log dir. */
  isCordoned: boolean;
}

export interface DescribeLogDirsResponseV5Body extends DescribeLogDirsResponseV4Body {
  logDirs: DescribeLogDirsResultV5[];
}

/**
 * DescribeLogDirs Response (Version: 5) => throttle_time_ms error_code [log_dirs] TAG_BUFFER
 *   throttle_time_ms => INT32
 *   error_code => INT16
 *   log_dirs => error_code log_dir [topics] total_bytes usable_bytes is_cordoned TAG_BUFFER
 *     error_code => INT16
 *     log_dir => COMPACT_STRING
 *     topics => see v2
 *     total_bytes => INT64
 *     usable_bytes => INT64
 *     is_cordoned => BOOLEAN
 *
 * v5 adds whether each log dir is cordoned.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const logDirSchema = flexibleObject([
  field('errorCode', int16),
  field('logDir', compactString),
  field('topics', compactArray(topicSchema)),
  field('totalBytes', int64),
  field('usableBytes', int64),
  field('isCordoned', boolean),
]);
export const responseSchema = flexibleObject([
  field('throttleTime', int32),
  field('errorCode', int16),
  field('logDirs', compactArray(logDirSchema)),
]);

export const describeLogDirsResponseV5 = defineResponse<DescribeLogDirsResponseV5Body>({
  schema: responseSchema,
  parse: parseDescribeLogDirsV3,
});
