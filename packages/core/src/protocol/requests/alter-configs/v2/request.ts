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

/**
 * AlterConfigs Request (Version: 2) => [resources] validate_only TAG_BUFFER
 *   resources => resource_type resource_name [configs] TAG_BUFFER
 *     resource_type => INT8
 *     resource_name => COMPACT_STRING
 *     configs => name value TAG_BUFFER
 *       name => COMPACT_STRING
 *       value => COMPACT_NULLABLE_STRING
 *   validate_only => BOOLEAN
 *
 * First flexible version (KIP-482). Same fields as v0/v1. Request header v2's trailing
 * TAG_BUFFER is written by `createRequest`, not here.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const configEntrySchema = flexibleObject([field('name', compactString), field('value', compactNullableString)]);
const resourceSchema = flexibleObject([
  field('type', int8),
  field('name', compactString),
  field('configEntries', compactArray(configEntrySchema)),
]);
export const requestSchema = flexibleObject([
  field('resources', compactArray(resourceSchema)),
  field('validateOnly', boolean),
]);

export const alterConfigsRequestV2 = defineRequest({
  apiKey: API_KEYS.AlterConfigs,
  apiVersion: 2,
  apiName: 'AlterConfigs',
  schema: requestSchema,
});
