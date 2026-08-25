import { boolean, compactBytes, defineRequest, field, flexibleObject, int8, int32, uuid } from '../../../schema';
import { API_KEYS } from '../../api-keys';

export interface PushTelemetryRequestV0Fields {
  clientInstanceId: Buffer;
  subscriptionId: number;
  terminating: boolean;
  compressionType: number;
  metrics: Buffer;
}

/**
 * PushTelemetry Request (Version: 0) => client_instance_id subscription_id terminating
 *   compression_type metrics TAG_BUFFER
 *     client_instance_id => UUID
 *     subscription_id => INT32
 *     terminating => BOOLEAN
 *     compression_type => INT8
 *     metrics => COMPACT_BYTES
 *
 * `metrics` is OTLP MetricsData v1 protobuf (KIP-714). Flexible from v0.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export const requestSchema = flexibleObject([
  field('clientInstanceId', uuid),
  field('subscriptionId', int32),
  field('terminating', boolean),
  field('compressionType', int8),
  field('metrics', compactBytes),
]);

export const pushTelemetryRequestV0 = defineRequest({
  apiKey: API_KEYS.PushTelemetry,
  apiVersion: 0,
  apiName: 'PushTelemetry',
  schema: requestSchema,
});
