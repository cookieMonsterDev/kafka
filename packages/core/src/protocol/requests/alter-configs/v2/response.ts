import { Decoder } from '../../../decoder';
import { createErrorFromCode, failure } from '../../../error-codes';
import {
  compactArray,
  compactNullableString,
  compactString,
  field,
  flexibleObject,
  int16,
  int32,
  int8,
} from '../../../schema';
import type { ResponseDefinition } from '../../../schema';
import type { AlterConfigsResponseV1Body } from '../v1/response';

export type AlterConfigsResponseV2Body = AlterConfigsResponseV1Body;

/**
 * AlterConfigs Response (Version: 2) => throttle_time_ms [responses] TAG_BUFFER
 *   throttle_time_ms => INT32
 *   responses => error_code error_message resource_type resource_name TAG_BUFFER
 *     error_code => INT16
 *     error_message => COMPACT_NULLABLE_STRING
 *     resource_type => INT8
 *     resource_name => COMPACT_STRING
 *
 * First flexible version (KIP-482). Same fields as v0/v1. Quota timing follows v1 (KIP-219).
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const resourceSchema = flexibleObject([
  field('errorCode', int16),
  field('errorMessage', compactNullableString),
  field('resourceType', int8),
  field('resourceName', compactString),
]);
const bodySchema = flexibleObject([field('throttleTime', int32), field('resources', compactArray(resourceSchema))]);

export const alterConfigsResponseV2: ResponseDefinition<AlterConfigsResponseV2Body> = {
  decode: async (rawData) => {
    const decoded = bodySchema.read(new Decoder(rawData));
    return { ...decoded, throttleTime: 0, clientSideThrottleTime: decoded.throttleTime };
  },
  parse: async (data) => {
    const resourceWithError = data.resources.find((resource) => failure(resource.errorCode));
    if (resourceWithError) throw createErrorFromCode(resourceWithError.errorCode);
    return data;
  },
};
