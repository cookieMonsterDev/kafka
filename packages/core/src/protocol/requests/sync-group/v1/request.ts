import { array, bytes, defineRequest, field, int32, object, string } from '../../../schema';
import { API_KEYS } from '../../api-keys';

const groupAssignmentSchema = object([field('memberId', string), field('memberAssignment', bytes)]);
const requestSchema = object([
  field('groupId', string),
  field('generationId', int32),
  field('memberId', string),
  field('groupAssignment', array(groupAssignmentSchema)),
]);

export const syncGroupRequestV1 = defineRequest({
  apiKey: API_KEYS.SyncGroup,
  apiVersion: 1,
  apiName: 'SyncGroup',
  schema: requestSchema,
});
