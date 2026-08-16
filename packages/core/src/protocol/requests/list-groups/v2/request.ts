import { defineRequest, object } from '../../../schema';
import { API_KEYS } from '../../api-keys';

const requestSchema = object([]);

export const listGroupsRequestV2 = defineRequest({
  apiKey: API_KEYS.ListGroups,
  apiVersion: 2,
  apiName: 'ListGroups',
  schema: requestSchema,
});
