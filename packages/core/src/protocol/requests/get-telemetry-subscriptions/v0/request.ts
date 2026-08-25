import { defineRequest, field, flexibleObject, uuid } from '../../../schema';
import { API_KEYS } from '../../api-keys';

export const ZERO_CLIENT_INSTANCE_ID = Buffer.alloc(16);

export interface GetTelemetrySubscriptionsRequestV0Fields {
  clientInstanceId: Buffer;
}

/**
 * GetTelemetrySubscriptions Request (Version: 0) => client_instance_id TAG_BUFFER
 *   client_instance_id => UUID
 *
 * Flexible from v0 (KIP-714). Send the all-zero UUID on the first request; the broker
 * assigns a stable id in the response.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export const requestSchema = flexibleObject([field('clientInstanceId', uuid)]);

export const getTelemetrySubscriptionsRequestV0 = defineRequest({
  apiKey: API_KEYS.GetTelemetrySubscriptions,
  apiVersion: 0,
  apiName: 'GetTelemetrySubscriptions',
  schema: requestSchema,
});
