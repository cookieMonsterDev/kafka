import { array, defineRequest, field, object, string } from '../../../schema';
import { API_KEYS } from '../../api-keys';

/**
 * DescribeGroups Request (Version: 0) => [group_ids]
 *   group_ids => STRING
 */
const requestSchema = object([field('groupIds', array(string))]);

export const describeGroupsRequestV0 = defineRequest({
  apiKey: API_KEYS.DescribeGroups,
  apiVersion: 0,
  apiName: 'DescribeGroups',
  schema: requestSchema,
});
