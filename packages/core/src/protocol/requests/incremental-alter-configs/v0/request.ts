import { array, boolean, defineRequest, field, int8, nullableString, object, string } from '../../../schema';
import { API_KEYS } from '../../api-keys';

export interface IncrementalAlterConfigsEntry {
  name: string;
  operation: number;
  value: string | null;
}

export interface IncrementalAlterConfigsResource {
  type: number;
  name: string;
  configs: IncrementalAlterConfigsEntry[];
}

export interface IncrementalAlterConfigsRequestV0Fields {
  resources: IncrementalAlterConfigsResource[];
  validateOnly: boolean;
}

/**
 * IncrementalAlterConfigs Request (Version: 0) => [resources] validate_only
 *   resources => resource_type resource_name [configs]
 *     resource_type => INT8
 *     resource_name => STRING
 *     configs => name config_operation value
 *       name => STRING
 *       config_operation => INT8
 *       value => NULLABLE_STRING
 *   validate_only => BOOLEAN
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const configSchema = object([field('name', string), field('operation', int8), field('value', nullableString)]);
const resourceSchema = object([field('type', int8), field('name', string), field('configs', array(configSchema))]);
export const requestSchema = object([field('resources', array(resourceSchema)), field('validateOnly', boolean)]);

export const incrementalAlterConfigsRequestV0 = defineRequest({
  apiKey: API_KEYS.IncrementalAlterConfigs,
  apiVersion: 0,
  apiName: 'IncrementalAlterConfigs',
  schema: requestSchema,
});
