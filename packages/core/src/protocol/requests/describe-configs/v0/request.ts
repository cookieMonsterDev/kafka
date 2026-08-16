import { array, defineRequest, field, int8, nullableArray, object, string } from '../../../schema';
import { API_KEYS } from '../../api-keys';

export interface DescribeConfigsRequestV0Fields {
  resources: { type: number; name: string; configNames: string[] }[];
}

/**
 * DescribeConfigs Request (Version: 0) => [resources]
 *   resources => resource_type resource_name [config_names]
 *     resource_type => INT8
 *     resource_name => STRING
 *     config_names => STRING
 *
 * `includeSynonyms` is not on the wire in this version and is ignored by the family factory.
 */
const resourceSchema = object([
  field('type', int8),
  field('name', string),
  field('configNames', nullableArray(string)),
]);
const requestSchema = object([field('resources', array(resourceSchema))]);

export const describeConfigsRequestV0 = defineRequest({
  apiKey: API_KEYS.DescribeConfigs,
  apiVersion: 0,
  apiName: 'DescribeConfigs',
  schema: requestSchema,
});
