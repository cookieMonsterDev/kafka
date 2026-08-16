import { boolean, compactString, defineRequest, field, flexibleObject, int16, int64 } from '../../../schema';
import { API_KEYS } from '../../api-keys';

/**
 * EndTxn Request (Version: 3) => transactional_id producer_id producer_epoch committed TAG_BUFFER
 *   transactional_id => COMPACT_STRING
 *   producer_id => INT64
 *   producer_epoch => INT16
 *   committed => BOOLEAN
 *
 * First flexible version (KIP-482). Same fields as v0–v2; decoded name stays `transactionResult`.
 * Request header v2's trailing TAG_BUFFER is written by `createRequest`, not here.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export const requestSchema = flexibleObject([
  field('transactionalId', compactString),
  field('producerId', int64),
  field('producerEpoch', int16),
  field('transactionResult', boolean),
]);

export const endTxnRequestV3 = defineRequest({
  apiKey: API_KEYS.EndTxn,
  apiVersion: 3,
  apiName: 'EndTxn',
  schema: requestSchema,
});
