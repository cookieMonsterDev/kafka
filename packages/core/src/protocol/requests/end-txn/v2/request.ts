import { boolean, defineRequest, field, int16, int64, object, string } from '../../../schema';
import { API_KEYS } from '../../api-keys';

/**
 * EndTxn Request (Version: 2) — same wire as v0/v1.
 * The bump may return PRODUCER_FENCED on the response.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const requestSchema = object([
  field('transactionalId', string),
  field('producerId', int64),
  field('producerEpoch', int16),
  field('transactionResult', boolean),
]);

export const endTxnRequestV2 = defineRequest({
  apiKey: API_KEYS.EndTxn,
  apiVersion: 2,
  apiName: 'EndTxn',
  schema: requestSchema,
});
