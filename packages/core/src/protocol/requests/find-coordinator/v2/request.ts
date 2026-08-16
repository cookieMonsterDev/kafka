import { defineRequest, field, int8, object, string } from '../../../schema';
import { API_KEYS } from '../../api-keys';

const requestSchema = object([field('coordinatorKey', string), field('coordinatorType', int8)]);

export const findCoordinatorRequestV2 = defineRequest({
  apiKey: API_KEYS.GroupCoordinator,
  apiVersion: 2,
  apiName: 'GroupCoordinator',
  schema: requestSchema,
});
