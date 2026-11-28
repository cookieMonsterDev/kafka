import { defineRequest } from '../../../schema';
import { API_KEYS } from '../../api-keys';
import { requestSchema } from '../v3/request';

/**
 * EndTxn Request (Version: 5) — same fields as version 3. Sent only under transaction version 2
 * (KIP-890 part 2): the coordinator bumps the producer epoch as it ends the transaction and
 * returns the new producer id and epoch.
 *
 * @see https://kafka.apache.org/44/design/protocol/
 */
export const endTxnRequestV5 = defineRequest({
  apiKey: API_KEYS.EndTxn,
  apiVersion: 5,
  apiName: 'EndTxn',
  schema: requestSchema,
});
