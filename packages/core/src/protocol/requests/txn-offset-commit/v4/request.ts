import { defineRequest } from '../../../schema';
import { API_KEYS } from '../../api-keys';
import { requestSchema } from '../v3/request';

/**
 * TxnOffsetCommit Request (Version: 4) — same fields as version 3. The bump tells the broker the
 * client handles TRANSACTION_ABORTABLE (KIP-890).
 *
 * @see https://kafka.apache.org/44/design/protocol/
 */
export const txnOffsetCommitRequestV4 = defineRequest({
  apiKey: API_KEYS.TxnOffsetCommit,
  apiVersion: 4,
  apiName: 'TxnOffsetCommit',
  schema: requestSchema,
});
