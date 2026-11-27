import { defineRequest } from '../../../schema';
import { API_KEYS } from '../../api-keys';
import { requestSchema } from '../v2/request';

export {
  type DescribeLogDirsRequestV2Fields as DescribeLogDirsRequestV5Fields,
  type DescribeLogDirsTopic,
} from '../v2/request';

/**
 * DescribeLogDirs Request (Version: 5) — wire format identical to v2; the response adds whether each log dir is cordoned.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export const describeLogDirsRequestV5 = defineRequest({
  apiKey: API_KEYS.DescribeLogDirs,
  apiVersion: 5,
  apiName: 'DescribeLogDirs',
  schema: requestSchema,
});
