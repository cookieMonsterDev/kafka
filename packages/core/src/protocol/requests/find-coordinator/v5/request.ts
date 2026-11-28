import { defineRequest } from '../../../schema';
import { API_KEYS } from '../../api-keys';
import { requestSchema } from '../v4/request';

/**
 * FindCoordinator Request (Version: 5) — same fields as version 4. The bump tells the broker the
 * client handles TRANSACTION_ABORTABLE (KIP-890).
 *
 * @see https://kafka.apache.org/44/design/protocol/
 */
export const findCoordinatorRequestV5 = defineRequest({
  apiKey: API_KEYS.GroupCoordinator,
  apiVersion: 5,
  apiName: 'GroupCoordinator',
  schema: requestSchema,
});
