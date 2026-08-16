import { compactNullableString, defineRequest, field, flexibleObject, int16, int32, int64 } from '../../../schema';
import { API_KEYS } from '../../api-keys';

/**
 * InitProducerId Request (Version: 4) => transactional_id transaction_timeout_ms producer_id producer_epoch TAG_BUFFER
 *   transactional_id => COMPACT_NULLABLE_STRING
 *   transaction_timeout_ms => INT32
 *   producer_id => INT64
 *   producer_epoch => INT16
 *
 * Same body as v3. Flexible (KIP-482). Request header v2's trailing TAG_BUFFER is written by
 * `createRequest`, not here.
 */
const requestSchema = flexibleObject([
  field('transactionalId', compactNullableString),
  field('transactionTimeout', int32),
  field('producerId', int64),
  field('producerEpoch', int16),
]);

export const initProducerIdRequestV4 = defineRequest({
  apiKey: API_KEYS.InitProducerId,
  apiVersion: 4,
  apiName: 'InitProducerId',
  schema: requestSchema,
});
