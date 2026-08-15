import { Decoder } from '../../../decoder.js';
import { createErrorFromCode, failure } from '../../../error-codes.js';
import { array, boolean, field, int16, int8, nullableString, object, string } from '../../../schema.js';
import type { ResponseDefinition } from '../../../schema.js';
import { CONFIG_SOURCE } from '../../../enums/config-source.js';

export interface DescribeConfigsSynonym {
  configName: string;
  configValue: string | null;
  configSource: number;
}

export interface DescribeConfigsEntry {
  configName: string;
  configValue: string | null;
  readOnly: boolean;
  isDefault: boolean;
  configSource: number;
  isSensitive: boolean;
  configSynonyms: DescribeConfigsSynonym[];
}

export interface DescribeConfigsResourceResult {
  errorCode: number;
  errorMessage: string | null;
  resourceType: number;
  resourceName: string;
  configEntries: DescribeConfigsEntry[];
}

export interface DescribeConfigsResponseV1Body {
  throttleTime: number;
  resources: DescribeConfigsResourceResult[];
}

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
 * DescribeConfigs Response (Version: 1) => throttle_time_ms [resources]
 *   throttle_time_ms => INT32
 *   resources => error_code error_message resource_type resource_name [config_entries]
 *     error_code => INT16
 *     error_message => NULLABLE_STRING
 *     resource_type => INT8
 *     resource_name => STRING
 *     config_entries => config_name config_value read_only config_source is_sensitive [config_synonyms]
 *       config_name => STRING
 *       config_value => NULLABLE_STRING
 *       read_only => BOOLEAN
 *       config_source => INT8
 *       is_sensitive => BOOLEAN
 *       config_synonyms => config_name config_value config_source
 *         config_name => STRING
 *         config_value => NULLABLE_STRING
 *         config_source => INT8
 *
 * `isDefault` isn't on the wire in this version — it's derived from `configSource`, which is why
 * this is a hand-written `ResponseDefinition` rather than a plain `defineResponse`: the schema
 * gives back exactly the wire fields, and `isDefault` needs a mapping pass over each entry.
 */
export const describeConfigsResponseV1: ResponseDefinition<DescribeConfigsResponseV1Body> = {
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
    return { throttleTime, resources };
  },
  parse: async (data) => {
    const resourceWithError = data.resources.find((resource) => failure(resource.errorCode));
    if (resourceWithError) throw createErrorFromCode(resourceWithError.errorCode);
    return data;
  },
};
