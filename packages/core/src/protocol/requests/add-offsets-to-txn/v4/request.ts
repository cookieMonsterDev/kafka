import { defineRequest } from '../../../schema';
import { API_KEYS } from '../../api-keys';
import { requestSchema } from '../v3/request';

/**
 * AddOffsetsToTxn Request (Version: 4) — same fields as version 3. The bump tells the broker the
 * client handles TRANSACTION_ABORTABLE (KIP-890).
 *
 * @see https://kafka.apache.org/44/design/protocol/
 */
export const addOffsetsToTxnRequestV4 = defineRequest({
  apiKey: API_KEYS.AddOffsetsToTxn,
  apiVersion: 4,
  apiName: 'AddOffsetsToTxn',
  schema: requestSchema,
});
