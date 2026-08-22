import { defineRequest, field, flexibleObject, int32 } from '../../../schema';
import { API_KEYS } from '../../api-keys';

export interface UnregisterBrokerRequestV0Fields {
  brokerId: number;
}

/**
 * UnregisterBroker Request (Version: 0) => broker_id TAG_BUFFER
 *   broker_id => INT32
 *
 * Flexible from v0.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export const requestSchema = flexibleObject([field('brokerId', int32)]);

export const unregisterBrokerRequestV0 = defineRequest({
  apiKey: API_KEYS.UnregisterBroker,
  apiVersion: 0,
  apiName: 'UnregisterBroker',
  schema: requestSchema,
});
