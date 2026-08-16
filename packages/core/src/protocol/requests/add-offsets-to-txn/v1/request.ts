import { defineRequest, field, int16, int64, object, string } from '../../../schema';
import { API_KEYS } from '../../api-keys';

/**
 * AddOffsetsToTxn Request (Version: 1) => transactional_id producer_id producer_epoch group_id
 *   transactional_id => STRING
 *   producer_id => INT64
 *   producer_epoch => INT16
 *   group_id => STRING
 */
const requestSchema = object([
  field('transactionalId', string),
  field('producerId', int64),
  field('producerEpoch', int16),
  field('groupId', string),
]);

export const addOffsetsToTxnRequestV1 = defineRequest({
  apiKey: API_KEYS.AddOffsetsToTxn,
  apiVersion: 1,
  apiName: 'AddOffsetsToTxn',
  schema: requestSchema,
});
