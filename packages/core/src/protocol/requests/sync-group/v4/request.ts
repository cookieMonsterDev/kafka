import {
  compactArray,
  compactBytes,
  compactNullableString,
  compactString,
  defineRequest,
  field,
  flexibleObject,
  int32,
} from '../../../schema';
import { API_KEYS } from '../../api-keys';

/**
 * SyncGroup Request (Version: 4) => group_id generation_id member_id group_instance_id [group_assignment] TAG_BUFFER
 *   group_id => COMPACT_STRING
 *   generation_id => INT32
 *   member_id => COMPACT_STRING
 *   group_instance_id => COMPACT_NULLABLE_STRING
 *   group_assignment => member_id member_assignment TAG_BUFFER
 *     member_id => COMPACT_STRING
 *     member_assignment => COMPACT_BYTES
 *
 * First flexible version (KIP-482). Same fields as v3. Request header v2's trailing TAG_BUFFER is
 * written by `createRequest`, not here.
 */
const groupAssignmentSchema = flexibleObject([
  field('memberId', compactString),
  field('memberAssignment', compactBytes),
]);
export const requestSchema = flexibleObject([
  field('groupId', compactString),
  field('generationId', int32),
  field('memberId', compactString),
  field('groupInstanceId', compactNullableString),
  field('groupAssignment', compactArray(groupAssignmentSchema)),
]);

export const syncGroupRequestV4 = defineRequest({
  apiKey: API_KEYS.SyncGroup,
  apiVersion: 4,
  apiName: 'SyncGroup',
  schema: requestSchema,
});
