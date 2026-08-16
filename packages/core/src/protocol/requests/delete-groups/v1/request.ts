import { array, defineRequest, field, object, string } from '../../../schema';
import { API_KEYS } from '../../api-keys';

const requestSchema = object([field('groupIds', array(string))]);

export const deleteGroupsRequestV1 = defineRequest({
  apiKey: API_KEYS.DeleteGroups,
  apiVersion: 1,
  apiName: 'DeleteGroups',
  schema: requestSchema,
});
