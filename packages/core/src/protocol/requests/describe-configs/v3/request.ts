import { array, boolean, defineRequest, field, int8, nullableArray, object, string } from '../../../schema';
import { API_KEYS } from '../../api-keys';

export { type DescribeConfigsResource, withDefaultConfigNames } from '../v1/request';

export interface DescribeConfigsRequestV3Fields {
  resources: { type: number; name: string; configNames: string[] }[];
  includeSynonyms: boolean;
  includeDocumentation: boolean;
}

/**
 * DescribeConfigs Request (Version: 3) => [resources] include_synonyms include_documentation
 *   resources => resource_type resource_name [config_names]
 *     resource_type => INT8
 *     resource_name => STRING
 *     config_names => STRING
 *   include_synonyms => BOOLEAN
 *   include_documentation => BOOLEAN
 *
 * KIP-524: `include_documentation` asks the broker to return per-entry `documentation`.
 */
const resourceSchema = object([
  field('type', int8),
  field('name', string),
  field('configNames', nullableArray(string)),
]);
export const requestSchema = object([
  field('resources', array(resourceSchema)),
  field('includeSynonyms', boolean),
  field('includeDocumentation', boolean),
]);

export const describeConfigsRequestV3 = defineRequest({
  apiKey: API_KEYS.DescribeConfigs,
  apiVersion: 3,
  apiName: 'DescribeConfigs',
  schema: requestSchema,
});
