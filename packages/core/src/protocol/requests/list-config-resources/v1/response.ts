import { Decoder } from '../../../decoder';
import { compactArray, compactString, field, flexibleObject, int8, int16, int32 } from '../../../schema';
import type { ResponseDefinition } from '../../../schema';
import {
  listConfigResourcesResponseV0,
  type ListConfigResource,
  type ListConfigResourcesResponseV0Body,
} from '../v0/response';

export type { ListConfigResource };
export type ListConfigResourcesResponseV1Body = ListConfigResourcesResponseV0Body;

/**
 * ListConfigResources Response (Version: 1) => throttle_time_ms error_code [config_resources] TAG_BUFFER
 *   config_resources => resource_name resource_type TAG_BUFFER
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const configResourceSchema = flexibleObject([field('resourceName', compactString), field('resourceType', int8)]);
export const responseSchema = flexibleObject([
  field('throttleTime', int32),
  field('errorCode', int16),
  field('configResources', compactArray(configResourceSchema)),
]);

export const listConfigResourcesResponseV1: ResponseDefinition<ListConfigResourcesResponseV1Body> = {
  decode: async (rawData) => {
    const decoded = responseSchema.read(new Decoder(rawData));
    return { ...decoded, throttleTime: 0, clientSideThrottleTime: decoded.throttleTime };
  },
  parse: async (data) => listConfigResourcesResponseV0.parse(data),
};
