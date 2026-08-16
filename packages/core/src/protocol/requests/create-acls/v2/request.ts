import { compactArray, compactString, defineRequest, field, flexibleObject, int8 } from '../../../schema';
import { API_KEYS } from '../../api-keys';

/**
 * CreateAcls Request (Version: 2) => [creations] TAG_BUFFER
 *   creations => resource_type resource_name resource_pattern_type principal host operation permission_type TAG_BUFFER
 *     resource_type => INT8
 *     resource_name => COMPACT_STRING
 *     resource_pattern_type => INT8
 *     principal => COMPACT_STRING
 *     host => COMPACT_STRING
 *     operation => INT8
 *     permission_type => INT8
 *
 * Flexible compact + tagged form of v1 (KIP-482). Request header v2's trailing TAG_BUFFER is
 * written by `createRequest`, not here.
 */
const creationSchema = flexibleObject([
  field('resourceType', int8),
  field('resourceName', compactString),
  field('resourcePatternType', int8),
  field('principal', compactString),
  field('host', compactString),
  field('operation', int8),
  field('permissionType', int8),
]);
export const requestSchema = flexibleObject([field('creations', compactArray(creationSchema))]);

export const createAclsRequestV2 = defineRequest({
  apiKey: API_KEYS.CreateAcls,
  apiVersion: 2,
  apiName: 'CreateAcls',
  schema: requestSchema,
});
