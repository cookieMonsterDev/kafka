import { defineRequest } from '../../../schema';
import { API_KEYS } from '../../api-keys';
import { requestSchema } from '../v3/request';

/**
 * TxnOffsetCommit Request (Version: 5) — same fields as versions 3 and 4. Inside a transaction it is sent
 * only under transaction version 2 (KIP-890 part 2), where it also adds the group's offsets to the
 * transaction, so no AddOffsetsToTxn comes first.
 *
 * @see https://kafka.apache.org/44/design/protocol/
 */
export const txnOffsetCommitRequestV5 = defineRequest({
  apiKey: API_KEYS.TxnOffsetCommit,
  apiVersion: 5,
  apiName: 'TxnOffsetCommit',
  schema: requestSchema,
});
