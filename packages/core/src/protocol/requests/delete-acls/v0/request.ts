import { API_KEYS } from '../../api-keys';
import { array, defineRequest, field, int8, nullableString, object } from '../../../schema';

/**
 * DeleteAcls Request (Version: 0) => [filters]
 *   filters => resource_type resource_name principal host operation permission_type
 *     resource_type => INT8
 *     resource_name => NULLABLE_STRING
 *     principal => NULLABLE_STRING
 *     host => NULLABLE_STRING
 *     operation => INT8
 *     permission_type => INT8
 */
const filterSchema = object([
  field('resourceType', int8),
  field('resourceName', nullableString),
  field('principal', nullableString),
  field('host', nullableString),
  field('operation', int8),
  field('permissionType', int8),
]);
const requestSchema = object([field('filters', array(filterSchema))]);

export const deleteAclsRequestV0 = defineRequest({
  apiKey: API_KEYS.DeleteAcls,
  apiVersion: 0,
  apiName: 'DeleteAcls',
  schema: requestSchema,
});
