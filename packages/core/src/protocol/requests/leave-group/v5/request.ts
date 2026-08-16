import {
  compactArray,
  compactNullableString,
  compactString,
  defineRequest,
  field,
  flexibleObject,
} from '../../../schema';
import { API_KEYS } from '../../api-keys';

/**
 * LeaveGroup Request (Version: 5) => group_id [members] TAG_BUFFER
 *   group_id => COMPACT_STRING
 *   members => member_id group_instance_id reason TAG_BUFFER
 *     member_id => COMPACT_STRING
 *     group_instance_id => COMPACT_NULLABLE_STRING
 *     reason => COMPACT_NULLABLE_STRING
 *
 * Adds Reason on each member (KIP-800).
 */
const memberSchema = flexibleObject([
  field('memberId', compactString),
  field('groupInstanceId', compactNullableString),
  field('reason', compactNullableString),
]);
export const requestSchema = flexibleObject([
  field('groupId', compactString),
  field('members', compactArray(memberSchema)),
]);

export const leaveGroupRequestV5 = defineRequest({
  apiKey: API_KEYS.LeaveGroup,
  apiVersion: 5,
  apiName: 'LeaveGroup',
  schema: requestSchema,
});
