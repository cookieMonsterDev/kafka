import { compactString, defineRequest, field, flexibleObject, int16, int64 } from '../../../schema';
import { API_KEYS } from '../../api-keys';

/**
 * AddOffsetsToTxn Request (Version: 3) => transactional_id producer_id producer_epoch group_id TAG_BUFFER
 *   transactional_id => COMPACT_STRING
 *   producer_id => INT64
 *   producer_epoch => INT16
 *   group_id => COMPACT_STRING
 *
 * First flexible version (KIP-482). Same fields as v0–v2. Request header v2's trailing
 * TAG_BUFFER is written by `createRequest`, not here.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export const requestSchema = flexibleObject([
  field('transactionalId', compactString),
  field('producerId', int64),
  field('producerEpoch', int16),
  field('groupId', compactString),
]);

export const addOffsetsToTxnRequestV3 = defineRequest({
  apiKey: API_KEYS.AddOffsetsToTxn,
  apiVersion: 3,
  apiName: 'AddOffsetsToTxn',
  schema: requestSchema,
});
