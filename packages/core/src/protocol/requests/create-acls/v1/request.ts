import { API_KEYS } from '../../api-keys.js';
import { array, defineRequest, field, int8, object, string } from '../../../schema.js';

/**
 * CreateAcls Request (Version: 1) => [creations]
 *   creations => resource_type resource_name resource_pattern_type principal host operation permission_type
 *     resource_type => INT8
 *     resource_name => STRING
 *     resource_pattern_type => INT8
 *     principal => STRING
 *     host => STRING
 *     operation => INT8
 *     permission_type => INT8
 */
const creationSchema = object([
  field('resourceType', int8),
  field('resourceName', string),
  field('resourcePatternType', int8),
  field('principal', string),
  field('host', string),
  field('operation', int8),
  field('permissionType', int8),
]);
const requestSchema = object([field('creations', array(creationSchema))]);

export const createAclsRequestV1 = defineRequest({
  apiKey: API_KEYS.CreateAcls,
  apiVersion: 1,
  apiName: 'CreateAcls',
  schema: requestSchema,
});
