import { defineRequest, field, int16, int32, int64, nullableString, object } from '../../../schema';
import { API_KEYS } from '../../api-keys';

/**
 * InitProducerId Request (Version: 2) => transactional_id transaction_timeout_ms producer_id producer_epoch
 *   transactional_id => NULLABLE_STRING
 *   transaction_timeout_ms => INT32
 *   producer_id => INT64
 *   producer_epoch => INT16
 *
 * KIP-360: producer_id + producer_epoch let the broker bump the epoch after UNKNOWN_PRODUCER_ID
 * instead of failing permanently. `-1` / `-1n` means "allocate a new producer id".
 */
const requestSchema = object([
  field('transactionalId', nullableString),
  field('transactionTimeout', int32),
  field('producerId', int64),
  field('producerEpoch', int16),
]);

export const initProducerIdRequestV2 = defineRequest({
  apiKey: API_KEYS.InitProducerId,
  apiVersion: 2,
  apiName: 'InitProducerId',
  schema: requestSchema,
});
