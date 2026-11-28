import { createErrorFromCode, failure } from '../../../error-codes';
import { compactArray, defineResponse, field, flexibleObject, int16, int32 } from '../../../schema';
import { describeLogDirsResponseV2, type DescribeLogDirsResponseV2Body, logDirSchema } from '../v2/response';

export interface DescribeLogDirsResponseV3Body extends DescribeLogDirsResponseV2Body {
  errorCode: number;
}

/**
 * DescribeLogDirs Response (Version: 3) => throttle_time_ms error_code [log_dirs] TAG_BUFFER
 *   throttle_time_ms => INT32
 *   error_code => INT16
 *   log_dirs => see v2
 *
 * v3 adds the top-level `error_code`, which reports a failure for the whole request (such as
 * CLUSTER_AUTHORIZATION_FAILED) that v2 signalled only by returning no log dirs.
 *
 * @see https://kafka.apache.org/44/design/protocol/
 */
export const responseSchema = flexibleObject([
  field('throttleTime', int32),
  field('errorCode', int16),
  field('logDirs', compactArray(logDirSchema)),
]);

/** Throws the top-level error, then the first log-dir error. Shared by v3 and later. */
export async function parseDescribeLogDirsV3<Body extends DescribeLogDirsResponseV3Body>(data: Body): Promise<Body> {
  if (failure(data.errorCode)) throw createErrorFromCode(data.errorCode);
  await describeLogDirsResponseV2.parse(data);
  return data;
}

export const describeLogDirsResponseV3 = defineResponse<DescribeLogDirsResponseV3Body>({
  schema: responseSchema,
  parse: parseDescribeLogDirsV3,
});
