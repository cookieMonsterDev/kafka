import { Decoder } from '../../../decoder';
import {
  compactArray,
  compactBytes,
  compactNullableString,
  compactString,
  field,
  flexibleObject,
  int16,
  int32,
} from '../../../schema';
import type { ResponseDefinition } from '../../../schema';
import { describeGroupsResponseV4, type DescribeGroupsResponseV4Body } from '../v4/response';

export type DescribeGroupsResponseV5Body = DescribeGroupsResponseV4Body;

/**
 * DescribeGroups Response (Version: 5) => throttle_time_ms [groups] TAG_BUFFER
 *   groups => error_code group_id group_state protocol_type protocol_data [members] authorized_operations TAG_BUFFER
 *     error_code => INT16
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
 * First flexible version (KIP-482). Same fields as v4. Decoded names stay `state` / `protocol`
 * to match this client. Quota timing follows v2 (KIP-219).
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const memberSchema = flexibleObject([
  field('memberId', compactString),
  field('groupInstanceId', compactNullableString),
  field('clientId', compactString),
  field('clientHost', compactString),
  field('memberMetadata', compactBytes),
  field('memberAssignment', compactBytes),
]);
const groupSchema = flexibleObject([
  field('errorCode', int16),
  field('groupId', compactString),
  field('state', compactString),
  field('protocolType', compactString),
  field('protocol', compactString),
  field('members', compactArray(memberSchema)),
  field('authorizedOperations', int32),
]);
const bodySchema = flexibleObject([field('throttleTime', int32), field('groups', compactArray(groupSchema))]);

export const describeGroupsResponseV5: ResponseDefinition<DescribeGroupsResponseV5Body> = {
  decode: async (rawData) => {
    const decoded = bodySchema.read(new Decoder(rawData));
    return { ...decoded, throttleTime: 0, clientSideThrottleTime: decoded.throttleTime };
  },
  parse: async (data) => {
    await describeGroupsResponseV4.parse(data);
    return data;
  },
};
