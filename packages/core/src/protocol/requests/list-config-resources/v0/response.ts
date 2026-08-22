import { Decoder } from '../../../decoder';
import { CONFIG_RESOURCE_TYPES } from '../../../enums/config-resource-types';
import { createErrorFromCode, failure } from '../../../error-codes';
import { compactArray, compactString, field, flexibleObject, int16, int32 } from '../../../schema';
import type { ResponseDefinition } from '../../../schema';

export interface ListConfigResource {
  resourceName: string;
  resourceType: number;
}

export interface ListConfigResourcesResponseV0Body {
  throttleTime: number;
  clientSideThrottleTime: number;
  errorCode: number;
  configResources: ListConfigResource[];
}

/**
 * ListConfigResources Response (Version: 0) => throttle_time_ms error_code [config_resources] TAG_BUFFER
 *   config_resources => resource_name TAG_BUFFER
 *
 * Version 0 is ListClientMetricsResources. ResourceType is ignorable on v1 with default 16
 * (CLIENT_METRICS), so v0 listings surface that type when decoded as the shared body.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const configResourceSchema = flexibleObject([field('resourceName', compactString)]);
export const responseSchema = flexibleObject([
  field('throttleTime', int32),
  field('errorCode', int16),
  field('configResources', compactArray(configResourceSchema)),
]);

export const listConfigResourcesResponseV0: ResponseDefinition<ListConfigResourcesResponseV0Body> = {
  decode: async (rawData) => {
    const decoded = responseSchema.read(new Decoder(rawData));
    return {
      ...decoded,
      throttleTime: 0,
      clientSideThrottleTime: decoded.throttleTime,
      configResources: decoded.configResources.map((resource) => ({
        ...resource,
        resourceType: CONFIG_RESOURCE_TYPES.CLIENT_METRICS,
      })),
    };
  },
  parse: async (data) => {
    if (failure(data.errorCode)) throw createErrorFromCode(data.errorCode);
    return data;
  },
};
