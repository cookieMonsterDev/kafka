import { defineRequest, field, int32, nullableString, object } from '../../../schema.js';
import { API_KEYS } from '../../api-keys.js';

/**
 * InitProducerId Request (Version: 0) => transactional_id transaction_timeout_ms
 *   transactional_id => NULLABLE_STRING
 *   transaction_timeout_ms => INT32
 */
const requestSchema = object([field('transactionalId', nullableString), field('transactionTimeout', int32)]);

export const initProducerIdRequestV0 = defineRequest({
  apiKey: API_KEYS.InitProducerId,
  apiVersion: 0,
  apiName: 'InitProducerId',
  schema: requestSchema,
});
