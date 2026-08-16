import { defineRequest, field, object, string } from '../../../schema';
import { API_KEYS } from '../../api-keys';

/**
 * FindCoordinator Request (Version: 0) => group_id
 *   group_id => STRING
 */
const requestSchema = object([field('groupId', string)]);

export const findCoordinatorRequestV0 = defineRequest({
  apiKey: API_KEYS.GroupCoordinator,
  apiVersion: 0,
  apiName: 'GroupCoordinator',
  schema: requestSchema,
});
