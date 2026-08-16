import { array, defineRequest, field, object, string } from '../../../schema';
import { API_KEYS } from '../../api-keys';

/**
 * DeleteGroups Request (Version: 0) => [groups_names]
 *   groups_names => STRING
 */
const requestSchema = object([field('groupIds', array(string))]);

export const deleteGroupsRequestV0 = defineRequest({
  apiKey: API_KEYS.DeleteGroups,
  apiVersion: 0,
  apiName: 'DeleteGroups',
  schema: requestSchema,
});
