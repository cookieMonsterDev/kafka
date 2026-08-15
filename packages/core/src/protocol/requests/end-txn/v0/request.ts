import { boolean, defineRequest, field, int16, int64, object, string } from '../../../schema.js';
import { API_KEYS } from '../../api-keys.js';

/**
 * EndTxn Request (Version: 0) => transactional_id producer_id producer_epoch transaction_result
 *   transactional_id => STRING
 *   producer_id => INT64
 *   producer_epoch => INT16
 *   transaction_result => BOOLEAN
 */
const requestSchema = object([
  field('transactionalId', string),
  field('producerId', int64),
  field('producerEpoch', int16),
  field('transactionResult', boolean),
]);

export const endTxnRequestV0 = defineRequest({
  apiKey: API_KEYS.EndTxn,
  apiVersion: 0,
  apiName: 'EndTxn',
  schema: requestSchema,
});
