import { defineRequest } from '../../../schema';
import { API_KEYS } from '../../api-keys';
import { requestSchema } from '../v2/request';

export {
  type DescribeLogDirsRequestV2Fields as DescribeLogDirsRequestV4Fields,
  type DescribeLogDirsTopic,
} from '../v2/request';

/**
 * DescribeLogDirs Request (Version: 4) — wire format identical to v2; the response adds volume sizes per log dir (KIP-827).
 *
 * @see https://kafka.apache.org/44/design/protocol/
 */
export const describeLogDirsRequestV4 = defineRequest({
  apiKey: API_KEYS.DescribeLogDirs,
  apiVersion: 4,
  apiName: 'DescribeLogDirs',
  schema: requestSchema,
});
