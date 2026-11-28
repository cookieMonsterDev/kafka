import { defineRequest } from '../../../schema';
import { API_KEYS } from '../../api-keys';
import { requestSchema } from '../v3/request';

/**
 * EndTxn Request (Version: 4) — same fields as version 3. The bump tells the broker the client
 * handles TRANSACTION_ABORTABLE (KIP-890).
 *
 * @see https://kafka.apache.org/44/design/protocol/
 */
export const endTxnRequestV4 = defineRequest({
  apiKey: API_KEYS.EndTxn,
  apiVersion: 4,
  apiName: 'EndTxn',
  schema: requestSchema,
});
