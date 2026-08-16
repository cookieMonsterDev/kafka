import { createErrorFromCode, failure } from '../../../error-codes';
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
import type { DescribeLogDirsResponseV0Body } from '../v0/response';

export type DescribeLogDirsResponseV2Body = DescribeLogDirsResponseV0Body;

/**
 * DescribeLogDirs Response (Version: 2) => throttle_time_ms [log_dirs] TAG_BUFFER
 *   throttle_time_ms => INT32
 *   log_dirs => error_code log_dir [topics] TAG_BUFFER
 *     error_code => INT16
 *     log_dir => COMPACT_STRING
 *     topics => topic [partitions] TAG_BUFFER
 *       topic => COMPACT_STRING
 *       partitions => partition size offset_lag is_future TAG_BUFFER
 *         partition => INT32
 *         size => INT64
 *         offset_lag => INT64
 *         is_future => BOOLEAN
 *
 * Flexible-version API. Response header v1's trailing TAG_BUFFER is skipped by `Connection`
 * before `decode()` runs.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const partitionSchema = flexibleObject([
  field('partition', int32),
  field('size', int64),
  field('offsetLag', int64),
  field('isFuture', boolean),
]);
const topicSchema = flexibleObject([field('topic', compactString), field('partitions', compactArray(partitionSchema))]);
const logDirSchema = flexibleObject([
  field('errorCode', int16),
  field('logDir', compactString),
  field('topics', compactArray(topicSchema)),
]);
export const responseSchema = flexibleObject([
  field('throttleTime', int32),
  field('logDirs', compactArray(logDirSchema)),
]);

export const describeLogDirsResponseV2 = defineResponse<DescribeLogDirsResponseV2Body>({
  schema: responseSchema,
  parse: async (data) => {
    const logDirWithError = data.logDirs.find((logDir) => failure(logDir.errorCode));
    if (logDirWithError) throw createErrorFromCode(logDirWithError.errorCode);
    return data;
  },
});
