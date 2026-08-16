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
 * SyncGroup Request (Version: 5) => group_id generation_id member_id group_instance_id protocol_type
 *                                   protocol_name [group_assignment] TAG_BUFFER
 *   group_id => COMPACT_STRING
 *   generation_id => INT32
 *   member_id => COMPACT_STRING
 *   group_instance_id => COMPACT_NULLABLE_STRING
 *   protocol_type => COMPACT_NULLABLE_STRING
 *   protocol_name => COMPACT_NULLABLE_STRING
 *   group_assignment => member_id member_assignment TAG_BUFFER
 *     member_id => COMPACT_STRING
 *     member_assignment => COMPACT_BYTES
 *
 * Adds ProtocolType and ProtocolName after groupInstanceId (KIP-559).
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
  field('protocolType', compactNullableString),
  field('protocolName', compactNullableString),
  field('groupAssignment', compactArray(groupAssignmentSchema)),
]);

export const syncGroupRequestV5 = defineRequest({
  apiKey: API_KEYS.SyncGroup,
  apiVersion: 5,
  apiName: 'SyncGroup',
  schema: requestSchema,
});
