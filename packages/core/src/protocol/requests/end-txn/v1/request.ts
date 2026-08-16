import { boolean, defineRequest, field, int16, int64, object, string } from '../../../schema';
import { API_KEYS } from '../../api-keys';

/**
 * EndTxn Request (Version: 1) => transactional_id producer_id producer_epoch transaction_result
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

export const endTxnRequestV1 = defineRequest({
  apiKey: API_KEYS.EndTxn,
  apiVersion: 1,
  apiName: 'EndTxn',
  schema: requestSchema,
});
