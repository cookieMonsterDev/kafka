import { defineRequest, object } from '../../../schema';
import { API_KEYS } from '../../api-keys';

/** ListGroups Request (Version: 0) — no fields. */
const requestSchema = object([]);

export const listGroupsRequestV0 = defineRequest({
  apiKey: API_KEYS.ListGroups,
  apiVersion: 0,
  apiName: 'ListGroups',
  schema: requestSchema,
});
