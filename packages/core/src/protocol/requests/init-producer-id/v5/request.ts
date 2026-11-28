import { defineRequest } from '../../../schema';
import { API_KEYS } from '../../api-keys';
import { requestSchema } from '../v4/request';

/**
 * InitProducerId Request (Version: 5) — same fields as version 4. The bump tells the broker the
 * client handles TRANSACTION_ABORTABLE (KIP-890).
 *
 * @see https://kafka.apache.org/44/design/protocol/
 */
export const initProducerIdRequestV5 = defineRequest({
  apiKey: API_KEYS.InitProducerId,
  apiVersion: 5,
  apiName: 'InitProducerId',
  schema: requestSchema,
});
