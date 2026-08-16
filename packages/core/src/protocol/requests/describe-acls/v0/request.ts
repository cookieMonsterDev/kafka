import { API_KEYS } from '../../api-keys';
import { defineRequest, field, int8, nullableString, object } from '../../../schema';

/**
 * DescribeAcls Request (Version: 0) => resource_type resource_name principal host operation permission_type
 *   resource_type => INT8
 *   resource_name => NULLABLE_STRING
 *   principal => NULLABLE_STRING
 *   host => NULLABLE_STRING
 *   operation => INT8
 *   permission_type => INT8
 */
const requestSchema = object([
  field('resourceType', int8),
  field('resourceName', nullableString),
  field('principal', nullableString),
  field('host', nullableString),
  field('operation', int8),
  field('permissionType', int8),
]);

export const describeAclsRequestV0 = defineRequest({
  apiKey: API_KEYS.DescribeAcls,
  apiVersion: 0,
  apiName: 'DescribeAcls',
  schema: requestSchema,
});
