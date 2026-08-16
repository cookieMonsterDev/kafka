import { defineRequest, field, int32, object, string } from '../../../schema';
import { API_KEYS } from '../../api-keys';

const requestSchema = object([field('groupId', string), field('groupGenerationId', int32), field('memberId', string)]);

export const heartbeatRequestV2 = defineRequest({
  apiKey: API_KEYS.Heartbeat,
  apiVersion: 2,
  apiName: 'Heartbeat',
  schema: requestSchema,
});
