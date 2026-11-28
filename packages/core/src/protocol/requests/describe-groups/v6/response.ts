import { Decoder } from '../../../decoder';
import { createErrorFromCode, failure } from '../../../error-codes';
import {
  compactArray,
  compactNullableString,
  compactString,
  field,
  flexibleObject,
  int16,
  int32,
} from '../../../schema';
import type { ResponseDefinition } from '../../../schema';
import type { DescribeGroupsResponseV4Group } from '../v4/response';
import { memberSchema } from '../v5/response';

export interface DescribeGroupsResponseV6Group extends DescribeGroupsResponseV4Group {
  errorMessage: string | null;
}

export interface DescribeGroupsResponseV6Body {
  throttleTime: number;
  clientSideThrottleTime: number;
  groups: DescribeGroupsResponseV6Group[];
}

/**
 * DescribeGroups Response (Version: 6) => throttle_time_ms [groups] TAG_BUFFER
 *   groups => error_code error_message group_id group_state protocol_type protocol_data [members] authorized_operations TAG_BUFFER
 *     error_code => INT16
 *     error_message => COMPACT_NULLABLE_STRING
 *     group_id => COMPACT_STRING
 *     group_state => COMPACT_STRING
 *     protocol_type => COMPACT_STRING
 *     protocol_data => COMPACT_STRING
 *     members => member_id group_instance_id client_id client_host member_metadata member_assignment TAG_BUFFER
 *       member_id => COMPACT_STRING
 *       group_instance_id => COMPACT_NULLABLE_STRING
 *       client_id => COMPACT_STRING
 *       client_host => COMPACT_STRING
 *       member_metadata => COMPACT_BYTES
 *       member_assignment => COMPACT_BYTES
 *     authorized_operations => INT32
 *
 * Adds `errorMessage` on each group. A group id the coordinator does not know as a classic group
 * comes back as GROUP_ID_NOT_FOUND instead of an empty `Dead` group (KIP-1043). Decoded names stay
 * `state` / `protocol` to match this client. Quota timing follows v2 (KIP-219).
 *
 * @see https://kafka.apache.org/44/design/protocol/
 */
const groupSchema = flexibleObject([
  field('errorCode', int16),
  field('errorMessage', compactNullableString),
  field('groupId', compactString),
  field('state', compactString),
  field('protocolType', compactString),
  field('protocol', compactString),
  field('members', compactArray(memberSchema)),
  field('authorizedOperations', int32),
]);
const bodySchema = flexibleObject([field('throttleTime', int32), field('groups', compactArray(groupSchema))]);

export const describeGroupsResponseV6: ResponseDefinition<DescribeGroupsResponseV6Body> = {
  decode: async (rawData) => {
    const decoded = bodySchema.read(new Decoder(rawData));
    return { ...decoded, throttleTime: 0, clientSideThrottleTime: decoded.throttleTime };
  },
  parse: async (data) => {
    const groupWithError = data.groups.find((group) => failure(group.errorCode));
    // The broker's message names the group and says whether it is missing or not a classic group.
    if (groupWithError) {
      throw createErrorFromCode(groupWithError.errorCode, { message: groupWithError.errorMessage });
    }
    return data;
  },
};
