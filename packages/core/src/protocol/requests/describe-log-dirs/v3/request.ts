import { defineRequest } from '../../../schema';
import { API_KEYS } from '../../api-keys';
import { requestSchema } from '../v2/request';

export {
  type DescribeLogDirsRequestV2Fields as DescribeLogDirsRequestV3Fields,
  type DescribeLogDirsTopic,
} from '../v2/request';

/**
 * DescribeLogDirs Request (Version: 3) — wire format identical to v2; the response adds a top-level error code.
 *
 * @see https://kafka.apache.org/44/design/protocol/
 */
export const describeLogDirsRequestV3 = defineRequest({
  apiKey: API_KEYS.DescribeLogDirs,
  apiVersion: 3,
  apiName: 'DescribeLogDirs',
  schema: requestSchema,
});
