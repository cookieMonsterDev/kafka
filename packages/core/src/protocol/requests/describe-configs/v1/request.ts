import { array, boolean, defineRequest, field, int8, nullableArray, object, string } from '../../../schema';
import { API_KEYS } from '../../api-keys';

export interface DescribeConfigsResource {
  type: number;
  name: string;
  configNames?: string[];
}

export interface DescribeConfigsRequestV1Fields {
  resources: { type: number; name: string; configNames: string[] }[];
  includeSynonyms: boolean;
}

/**
 * DescribeConfigs Request (Version: 1) => [resources] include_synonyms
 *   resources => resource_type resource_name [config_names]
 *     resource_type => INT8
 *     resource_name => STRING
 *     config_names => STRING
 *   include_synonyms => BOOLEAN
 */
const resourceSchema = object([
  field('type', int8),
  field('name', string),
  field('configNames', nullableArray(string)),
]);
export const requestSchema = object([field('resources', array(resourceSchema)), field('includeSynonyms', boolean)]);

export const describeConfigsRequestV1 = defineRequest({
  apiKey: API_KEYS.DescribeConfigs,
  apiVersion: 1,
  apiName: 'DescribeConfigs',
  schema: requestSchema,
});

export function withDefaultConfigNames(
  resources: readonly DescribeConfigsResource[],
): { type: number; name: string; configNames: string[] }[] {
  return resources.map(({ type, name, configNames = [] }) => ({ type, name, configNames }));
}
