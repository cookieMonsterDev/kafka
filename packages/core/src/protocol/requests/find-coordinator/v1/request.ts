import { defineRequest, field, int8, object, string } from '../../../schema';
import { API_KEYS } from '../../api-keys';

/**
 * FindCoordinator Request (Version: 1) => coordinator_key coordinator_type
 *   coordinator_key => STRING
 *   coordinator_type => INT8
 */
const requestSchema = object([field('coordinatorKey', string), field('coordinatorType', int8)]);

export const findCoordinatorRequestV1 = defineRequest({
  apiKey: API_KEYS.GroupCoordinator,
  apiVersion: 1,
  apiName: 'GroupCoordinator',
  schema: requestSchema,
});
