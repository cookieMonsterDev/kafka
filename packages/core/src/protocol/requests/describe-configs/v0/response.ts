import { Decoder } from '../../../decoder';
import { CONFIG_RESOURCE_TYPES } from '../../../enums/config-resource-types';
import { CONFIG_SOURCE } from '../../../enums/config-source';
import { createErrorFromCode, failure } from '../../../error-codes';
import { array, boolean, field, int16, int8, nullableString, object, string } from '../../../schema';
import type { ResponseDefinition } from '../../../schema';

export interface DescribeConfigsResponseV0Entry {
  configName: string;
  configValue: string | null;
  readOnly: boolean;
  isDefault: boolean;
  configSource: number;
  isSensitive: boolean;
  configSynonyms: [];
}

export interface DescribeConfigsResponseV0Body {
  throttleTime: number;
  resources: {
    errorCode: number;
    errorMessage: string | null;
    resourceType: number;
    resourceName: string;
    configEntries: DescribeConfigsResponseV0Entry[];
  }[];
}

const rawConfigEntrySchema = object([
  field('configName', string),
  field('configValue', nullableString),
  field('readOnly', boolean),
  field('isDefault', boolean),
  field('isSensitive', boolean),
]);
const rawResourceSchema = object([
  field('errorCode', int16),
  field('errorMessage', nullableString),
  field('resourceType', int8),
  field('resourceName', string),
  field('configEntries', array(rawConfigEntrySchema)),
]);
const resourcesSchema = array(rawResourceSchema);

/**
 * Backport `configSource` from the v0 `is_default` flag and resource type.
 * @see https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/common/requests/DescribeConfigsResponse.java
 */
function configSourceFromV0(isDefault: boolean, resourceType: number): number {
  if (isDefault) return CONFIG_SOURCE.DEFAULT_CONFIG;
  switch (resourceType) {
    case CONFIG_RESOURCE_TYPES.BROKER:
      return CONFIG_SOURCE.STATIC_BROKER_CONFIG;
    case CONFIG_RESOURCE_TYPES.TOPIC:
      return CONFIG_SOURCE.TOPIC_CONFIG;
    default:
      return CONFIG_SOURCE.UNKNOWN;
  }
}

/**
 * DescribeConfigs Response (Version: 0) => throttle_time_ms [resources]
 *   throttle_time_ms => INT32
 *   resources => error_code error_message resource_type resource_name [config_entries]
 *     error_code => INT16
 *     error_message => NULLABLE_STRING
 *     resource_type => INT8
 *     resource_name => STRING
 *     config_entries => config_name config_value read_only is_default is_sensitive
 *       config_name => STRING
 *       config_value => NULLABLE_STRING
 *       read_only => BOOLEAN
 *       is_default => BOOLEAN
 *       is_sensitive => BOOLEAN
 *
 * Synonyms are not on the wire; decoded entries expose an empty `configSynonyms` array so
 * callers can share the v1+ result shape.
 */
export const describeConfigsResponseV0: ResponseDefinition<DescribeConfigsResponseV0Body> = {
  decode: async (rawData) => {
    const decoder = new Decoder(rawData);
    const throttleTime = decoder.readInt32();
    const rawResources = resourcesSchema.read(decoder);
    const resources = rawResources.map((resource) => ({
      ...resource,
      configEntries: resource.configEntries.map((entry) => ({
        ...entry,
        configSource: configSourceFromV0(entry.isDefault, resource.resourceType),
        configSynonyms: [] as [],
      })),
    }));
    return { throttleTime, resources };
  },
  parse: async (data) => {
    const resourceWithError = data.resources.find((resource) => failure(resource.errorCode));
    if (resourceWithError) throw createErrorFromCode(resourceWithError.errorCode);
    return data;
  },
};
