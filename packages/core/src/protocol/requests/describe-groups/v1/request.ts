import { array, defineRequest, field, object, string } from '../../../schema.js';
import { API_KEYS } from '../../api-keys.js';

const requestSchema = object([field('groupIds', array(string))]);

export const describeGroupsRequestV1 = defineRequest({
  apiKey: API_KEYS.DescribeGroups,
  apiVersion: 1,
  apiName: 'DescribeGroups',
  schema: requestSchema,
});
