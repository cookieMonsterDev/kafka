import { Decoder } from '../../../decoder';
import { CONFIG_SOURCE } from '../../../enums/config-source';
import { createErrorFromCode, failure } from '../../../error-codes';
import {
  boolean,
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
import type { DescribeConfigsResponseV3Body } from '../v3/response';

export type DescribeConfigsResponseV4Body = DescribeConfigsResponseV3Body;

const synonymSchema = flexibleObject([
  field('configName', compactString),
  field('configValue', compactNullableString),
  field('configSource', int8),
]);
const rawConfigEntrySchema = flexibleObject([
  field('configName', compactString),
  field('configValue', compactNullableString),
  field('readOnly', boolean),
  field('configSource', int8),
  field('isSensitive', boolean),
  field('configSynonyms', compactArray(synonymSchema)),
  field('configType', int8),
  field('documentation', compactNullableString),
]);
const rawResourceSchema = flexibleObject([
  field('errorCode', int16),
  field('errorMessage', compactNullableString),
  field('resourceType', int8),
  field('resourceName', compactString),
  field('configEntries', compactArray(rawConfigEntrySchema)),
]);
const bodySchema = flexibleObject([field('throttleTime', int32), field('resources', compactArray(rawResourceSchema))]);

/**
 * DescribeConfigs Response (Version: 4) => throttle_time_ms [resources] TAG_BUFFER
 *   throttle_time_ms => INT32
 *   resources => error_code error_message resource_type resource_name [config_entries] TAG_BUFFER
 *     error_code => INT16
 *     error_message => COMPACT_NULLABLE_STRING
 *     resource_type => INT8
 *     resource_name => COMPACT_STRING
 *     config_entries => config_name config_value read_only config_source is_sensitive [config_synonyms] config_type documentation TAG_BUFFER
 *       config_name => COMPACT_STRING
 *       config_value => COMPACT_NULLABLE_STRING
 *       read_only => BOOLEAN
 *       config_source => INT8
 *       is_sensitive => BOOLEAN
 *       config_synonyms => config_name config_value config_source TAG_BUFFER
 *         config_name => COMPACT_STRING
 *         config_value => COMPACT_NULLABLE_STRING
 *         config_source => INT8
 *       config_type => INT8
 *       documentation => COMPACT_NULLABLE_STRING
 *
 * Flexible compact + tagged form of v3. `isDefault` is derived from `configSource` like v1/v3.
 * Response header v1's trailing TAG_BUFFER is skipped by `Connection` before `decode()` runs.
 * Quota timing follows v2 (KIP-219).
 */
export const describeConfigsResponseV4: ResponseDefinition<DescribeConfigsResponseV4Body> = {
  decode: async (rawData) => {
    const decoded = bodySchema.read(new Decoder(rawData));
    const resources = decoded.resources.map((resource) => ({
      ...resource,
      configEntries: resource.configEntries.map((entry) => ({
        ...entry,
        isDefault: entry.configSource === CONFIG_SOURCE.DEFAULT_CONFIG,
      })),
    }));
    return { throttleTime: 0, clientSideThrottleTime: decoded.throttleTime, resources };
  },
  parse: async (data) => {
    const resourceWithError = data.resources.find((resource) => failure(resource.errorCode));
    if (resourceWithError) throw createErrorFromCode(resourceWithError.errorCode);
    return data;
  },
};
