import { defineRequest, object } from '../../../schema.js';
import { API_KEYS } from '../../api-keys.js';

const requestSchema = object([]);

export const listGroupsRequestV2 = defineRequest({
  apiKey: API_KEYS.ListGroups,
  apiVersion: 2,
  apiName: 'ListGroups',
  schema: requestSchema,
});
