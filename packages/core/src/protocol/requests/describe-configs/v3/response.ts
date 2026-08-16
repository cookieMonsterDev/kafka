import { Decoder } from '../../../decoder';
import { createErrorFromCode, failure } from '../../../error-codes';
import { array, boolean, field, int16, int8, nullableString, object, string } from '../../../schema';
import type { ResponseDefinition } from '../../../schema';
import { CONFIG_SOURCE } from '../../../enums/config-source';
import type { DescribeConfigsResponseV2Body } from '../v2/response';

export type { DescribeConfigsEntry, DescribeConfigsSynonym } from '../v1/response';

export type DescribeConfigsResponseV3Body = DescribeConfigsResponseV2Body;

const synonymSchema = object([
  field('configName', string),
  field('configValue', nullableString),
  field('configSource', int8),
]);
const rawConfigEntrySchema = object([
  field('configName', string),
  field('configValue', nullableString),
  field('readOnly', boolean),
  field('configSource', int8),
  field('isSensitive', boolean),
  field('configSynonyms', array(synonymSchema)),
  field('configType', int8),
  field('documentation', nullableString),
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
 * DescribeConfigs Response (Version: 3) => throttle_time_ms [resources]
 *   throttle_time_ms => INT32
 *   resources => error_code error_message resource_type resource_name [config_entries]
 *     error_code => INT16
 *     error_message => NULLABLE_STRING
 *     resource_type => INT8
 *     resource_name => STRING
 *     config_entries => config_name config_value read_only config_source is_sensitive [config_synonyms] config_type documentation
 *       config_name => STRING
 *       config_value => NULLABLE_STRING
 *       read_only => BOOLEAN
 *       config_source => INT8
 *       is_sensitive => BOOLEAN
 *       config_synonyms => config_name config_value config_source
 *         config_name => STRING
 *         config_value => NULLABLE_STRING
 *         config_source => INT8
 *       config_type => INT8
 *       documentation => NULLABLE_STRING
 *
 * KIP-524 adds `config_type` and `documentation` after synonyms. `isDefault` is still derived
 * from `configSource` (same as v1), which is why this is a hand-written `ResponseDefinition`.
 * Quota timing follows v2 (KIP-219): the decoded throttle is exposed as `clientSideThrottleTime`.
 */
export const describeConfigsResponseV3: ResponseDefinition<DescribeConfigsResponseV3Body> = {
  decode: async (rawData) => {
    const decoder = new Decoder(rawData);
    const throttleTime = decoder.readInt32();
    const rawResources = resourcesSchema.read(decoder);
    const resources = rawResources.map((resource) => ({
      ...resource,
      configEntries: resource.configEntries.map((entry) => ({
        ...entry,
        isDefault: entry.configSource === CONFIG_SOURCE.DEFAULT_CONFIG,
      })),
    }));
    return { throttleTime: 0, clientSideThrottleTime: throttleTime, resources };
  },
  parse: async (data) => {
    const resourceWithError = data.resources.find((resource) => failure(resource.errorCode));
    if (resourceWithError) throw createErrorFromCode(resourceWithError.errorCode);
    return data;
  },
};
