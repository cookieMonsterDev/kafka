import { API_KEYS } from '../../api-keys';
import { array, defineRequest, field, int8, object, string } from '../../../schema';

/**
 * CreateAcls Request (Version: 0) => [creations]
 *   creations => resource_type resource_name principal host operation permission_type
 *     resource_type => INT8
 *     resource_name => STRING
 *     principal => STRING
 *     host => STRING
 *     operation => INT8
 *     permission_type => INT8
 */
const creationSchema = object([
  field('resourceType', int8),
  field('resourceName', string),
  field('principal', string),
  field('host', string),
  field('operation', int8),
  field('permissionType', int8),
]);
const requestSchema = object([field('creations', array(creationSchema))]);

export const createAclsRequestV0 = defineRequest({
  apiKey: API_KEYS.CreateAcls,
  apiVersion: 0,
  apiName: 'CreateAcls',
  schema: requestSchema,
});
