import { defineRequest, object } from '../../../schema';
import { API_KEYS } from '../../api-keys';

const requestSchema = object([]);

export const listGroupsRequestV1 = defineRequest({
  apiKey: API_KEYS.ListGroups,
  apiVersion: 1,
  apiName: 'ListGroups',
  schema: requestSchema,
});
