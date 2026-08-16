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
 * LeaveGroup Request (Version: 4) => group_id [members] TAG_BUFFER
 *   group_id => COMPACT_STRING
 *   members => member_id group_instance_id TAG_BUFFER
 *     member_id => COMPACT_STRING
 *     group_instance_id => COMPACT_NULLABLE_STRING
 *
 * First flexible version (KIP-482). Same batched members as v3. Request header v2's trailing
 * TAG_BUFFER is written by `createRequest`, not here.
 */
const memberSchema = flexibleObject([
  field('memberId', compactString),
  field('groupInstanceId', compactNullableString),
]);
export const requestSchema = flexibleObject([
  field('groupId', compactString),
  field('members', compactArray(memberSchema)),
]);

export const leaveGroupRequestV4 = defineRequest({
  apiKey: API_KEYS.LeaveGroup,
  apiVersion: 4,
  apiName: 'LeaveGroup',
  schema: requestSchema,
});
