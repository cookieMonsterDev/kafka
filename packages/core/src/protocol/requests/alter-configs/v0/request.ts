import { array, boolean, defineRequest, field, int8, nullableString, object, string } from '../../../schema.js';
import { API_KEYS } from '../../api-keys.js';

export interface AlterConfigsEntry {
  name: string;
  value: string | null;
}

export interface AlterConfigsResource {
  type: number;
  name: string;
  configEntries: AlterConfigsEntry[];
}

export interface AlterConfigsRequestV0Fields {
  resources: AlterConfigsResource[];
  validateOnly: boolean;
}

/**
 * AlterConfigs Request (Version: 0) => [resources] validate_only
 *   resources => resource_type resource_name [config_entries]
 *     resource_type => INT8
 *     resource_name => STRING
 *     config_entries => config_name config_value
 *       config_name => STRING
 *       config_value => NULLABLE_STRING
 *   validate_only => BOOLEAN
 */
const configEntrySchema = object([field('name', string), field('value', nullableString)]);
const resourceSchema = object([
  field('type', int8),
  field('name', string),
  field('configEntries', array(configEntrySchema)),
]);
export const requestSchema = object([field('resources', array(resourceSchema)), field('validateOnly', boolean)]);

export const alterConfigsRequestV0 = defineRequest({
  apiKey: API_KEYS.AlterConfigs,
  apiVersion: 0,
  apiName: 'AlterConfigs',
  schema: requestSchema,
});
