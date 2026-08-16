import {
  boolean,
  compactArray,
  compactNullableString,
  compactString,
  defineRequest,
  field,
  flexibleObject,
  int8,
} from '../../../schema';
import { API_KEYS } from '../../api-keys';

export {
  type IncrementalAlterConfigsEntry,
  type IncrementalAlterConfigsRequestV0Fields as IncrementalAlterConfigsRequestV1Fields,
  type IncrementalAlterConfigsResource,
} from '../v0/request';

/**
 * IncrementalAlterConfigs Request (Version: 1) => [resources] validate_only TAG_BUFFER
 *   resources => resource_type resource_name [configs] TAG_BUFFER
 *     resource_type => INT8
 *     resource_name => COMPACT_STRING
 *     configs => name config_operation value TAG_BUFFER
 *       name => COMPACT_STRING
 *       config_operation => INT8
 *       value => COMPACT_NULLABLE_STRING
 *   validate_only => BOOLEAN
 *
 * Flexible-version API. Request header v2's trailing TAG_BUFFER is written by `createRequest`.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const configSchema = flexibleObject([
  field('name', compactString),
  field('operation', int8),
  field('value', compactNullableString),
]);
const resourceSchema = flexibleObject([
  field('type', int8),
  field('name', compactString),
  field('configs', compactArray(configSchema)),
]);
export const requestSchema = flexibleObject([
  field('resources', compactArray(resourceSchema)),
  field('validateOnly', boolean),
]);

export const incrementalAlterConfigsRequestV1 = defineRequest({
  apiKey: API_KEYS.IncrementalAlterConfigs,
  apiVersion: 1,
  apiName: 'IncrementalAlterConfigs',
  schema: requestSchema,
});
