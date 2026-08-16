import { defineRequest } from '../../../schema';
import { API_KEYS } from '../../api-keys';
import { requestSchema } from '../v0/request';

export {
  type DescribeLogDirsRequestV0Fields as DescribeLogDirsRequestV1Fields,
  type DescribeLogDirsTopic,
} from '../v0/request';

/**
 * DescribeLogDirs Request (Version: 1) — wire format identical to v0; only the response's
 * throttling semantics change (KIP-219).
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export const describeLogDirsRequestV1 = defineRequest({
  apiKey: API_KEYS.DescribeLogDirs,
  apiVersion: 1,
  apiName: 'DescribeLogDirs',
  schema: requestSchema,
});
