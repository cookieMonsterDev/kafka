import { createErrorFromCode, failure } from '../../../error-codes';
import { array, boolean, defineResponse, field, int16, int32, int64, object, string } from '../../../schema';

export interface DescribeLogDirsPartition {
  partition: number;
  size: bigint;
  offsetLag: bigint;
  isFuture: boolean;
}

export interface DescribeLogDirsTopicResult {
  topic: string;
  partitions: DescribeLogDirsPartition[];
}

export interface DescribeLogDirsResult {
  errorCode: number;
  logDir: string;
  topics: DescribeLogDirsTopicResult[];
}

export interface DescribeLogDirsResponseV0Body {
  throttleTime: number;
  logDirs: DescribeLogDirsResult[];
}

/**
 * DescribeLogDirs Response (Version: 0) => throttle_time_ms [log_dirs]
 *   throttle_time_ms => INT32
 *   log_dirs => error_code log_dir [topics]
 *     error_code => INT16
 *     log_dir => STRING
 *     topics => topic [partitions]
 *       topic => STRING
 *       partitions => partition size offset_lag is_future
 *         partition => INT32
 *         size => INT64
 *         offset_lag => INT64
 *         is_future => BOOLEAN
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const partitionSchema = object([
  field('partition', int32),
  field('size', int64),
  field('offsetLag', int64),
  field('isFuture', boolean),
]);
const topicSchema = object([field('topic', string), field('partitions', array(partitionSchema))]);
const logDirSchema = object([field('errorCode', int16), field('logDir', string), field('topics', array(topicSchema))]);
export const responseSchema = object([field('throttleTime', int32), field('logDirs', array(logDirSchema))]);

export const describeLogDirsResponseV0 = defineResponse<DescribeLogDirsResponseV0Body>({
  schema: responseSchema,
  parse: async (data) => {
    const logDirWithError = data.logDirs.find((logDir) => failure(logDir.errorCode));
    if (logDirWithError) throw createErrorFromCode(logDirWithError.errorCode);
    return data;
  },
});
