import { defineRequest, field, flexibleObject, int32 } from '../../../schema';
import { API_KEYS } from '../../api-keys';

export interface UnregisterControllerRequestV0Fields {
  controllerId: number;
}

/**
 * UnregisterController Request (Version: 0) => controller_id TAG_BUFFER
 *   controller_id => INT32
 *
 * Flexible from v0 (KIP-1312, Kafka 4.4).
 *
 * @see https://kafka.apache.org/44/design/protocol/
 */
export const requestSchema = flexibleObject([field('controllerId', int32)]);

export const unregisterControllerRequestV0 = defineRequest({
  apiKey: API_KEYS.UnregisterController,
  apiVersion: 0,
  apiName: 'UnregisterController',
  schema: requestSchema,
});
