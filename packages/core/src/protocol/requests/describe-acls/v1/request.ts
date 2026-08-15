import { API_KEYS } from '../../api-keys.js';
import { defineRequest, field, int8, nullableString, object } from '../../../schema.js';

/**
 * DescribeAcls Request (Version: 1) => resource_type resource_name resource_pattern_type_filter principal host operation permission_type
 *   resource_type => INT8
 *   resource_name => NULLABLE_STRING
 *   resource_pattern_type_filter => INT8
 *   principal => NULLABLE_STRING
 *   host => NULLABLE_STRING
 *   operation => INT8
 *   permission_type => INT8
 */
const requestSchema = object([
  field('resourceType', int8),
  field('resourceName', nullableString),
  field('resourcePatternType', int8),
  field('principal', nullableString),
  field('host', nullableString),
  field('operation', int8),
  field('permissionType', int8),
]);

export const describeAclsRequestV1 = defineRequest({
  apiKey: API_KEYS.DescribeAcls,
  apiVersion: 1,
  apiName: 'DescribeAcls',
  schema: requestSchema,
});
