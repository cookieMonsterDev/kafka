import { compactNullableString, defineRequest, field, flexibleObject, int8 } from '../../../schema';
import { API_KEYS } from '../../api-keys';

/**
 * DescribeAcls Request (Version: 2) => resource_type resource_name resource_pattern_type_filter principal host operation permission_type TAG_BUFFER
 *   resource_type => INT8
 *   resource_name => COMPACT_NULLABLE_STRING
 *   resource_pattern_type_filter => INT8
 *   principal => COMPACT_NULLABLE_STRING
 *   host => COMPACT_NULLABLE_STRING
 *   operation => INT8
 *   permission_type => INT8
 *
 * Flexible compact + tagged form of v1 (KIP-482). Request header v2's trailing TAG_BUFFER is
 * written by `createRequest`, not here.
 */
export const requestSchema = flexibleObject([
  field('resourceType', int8),
  field('resourceName', compactNullableString),
  field('resourcePatternType', int8),
  field('principal', compactNullableString),
  field('host', compactNullableString),
  field('operation', int8),
  field('permissionType', int8),
]);

export const describeAclsRequestV2 = defineRequest({
  apiKey: API_KEYS.DescribeAcls,
  apiVersion: 2,
  apiName: 'DescribeAcls',
  schema: requestSchema,
});
