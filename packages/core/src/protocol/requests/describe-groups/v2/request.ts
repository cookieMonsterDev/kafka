import { array, defineRequest, field, object, string } from '../../../schema';
import { API_KEYS } from '../../api-keys';

const requestSchema = object([field('groupIds', array(string))]);

export const describeGroupsRequestV2 = defineRequest({
  apiKey: API_KEYS.DescribeGroups,
  apiVersion: 2,
  apiName: 'DescribeGroups',
  schema: requestSchema,
});
