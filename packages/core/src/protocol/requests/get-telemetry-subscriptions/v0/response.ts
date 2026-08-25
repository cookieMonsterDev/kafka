import { Decoder } from '../../../decoder';
import { createErrorFromCode, failure } from '../../../error-codes';
import { boolean, compactArray, compactString, field, flexibleObject, int8, int16, int32, uuid } from '../../../schema';
import type { ResponseDefinition } from '../../../schema';

export interface GetTelemetrySubscriptionsResponseV0Body {
  throttleTime: number;
  clientSideThrottleTime: number;
  errorCode: number;
  clientInstanceId: Buffer;
  subscriptionId: number;
  acceptedCompressionTypes: number[];
  pushIntervalMs: number;
  telemetryMaxBytes: number;
  deltaTemporality: boolean;
  requestedMetrics: string[];
}

/**
 * GetTelemetrySubscriptions Response (Version: 0) => throttle_time_ms error_code client_instance_id
 *   subscription_id [accepted_compression_types] push_interval_ms telemetry_max_bytes
 *   delta_temporality [requested_metrics] TAG_BUFFER
 *     throttle_time_ms => INT32
 *     error_code => INT16
 *     client_instance_id => UUID
 *     subscription_id => INT32
 *     accepted_compression_types => INT8
 *     push_interval_ms => INT32
 *     telemetry_max_bytes => INT32
 *     delta_temporality => BOOLEAN
 *     requested_metrics => COMPACT_STRING
 *
 * Empty `requestedMetrics`: push nothing. A single empty string: push all client metrics.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export const responseSchema = flexibleObject([
  field('throttleTime', int32),
  field('errorCode', int16),
  field('clientInstanceId', uuid),
  field('subscriptionId', int32),
  field('acceptedCompressionTypes', compactArray(int8)),
  field('pushIntervalMs', int32),
  field('telemetryMaxBytes', int32),
  field('deltaTemporality', boolean),
  field('requestedMetrics', compactArray(compactString)),
]);

export const getTelemetrySubscriptionsResponseV0: ResponseDefinition<GetTelemetrySubscriptionsResponseV0Body> = {
  decode: async (rawData) => {
    const decoded = responseSchema.read(new Decoder(rawData));
    return { ...decoded, throttleTime: 0, clientSideThrottleTime: decoded.throttleTime };
  },
  parse: async (data) => {
    if (failure(data.errorCode)) throw createErrorFromCode(data.errorCode);
    return data;
  },
};
