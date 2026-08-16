import { Decoder } from '../../../decoder';
import { array, bytes, field, int16, int32, nullableString, object, string } from '../../../schema';
import type { ResponseDefinition } from '../../../schema';
import { describeGroupsResponseV3 } from '../v3/response';

export interface DescribeGroupsResponseV4Member {
  memberId: string;
  groupInstanceId: string | null;
  clientId: string;
  clientHost: string;
  memberMetadata: Buffer;
  memberAssignment: Buffer;
}

export interface DescribeGroupsResponseV4Group {
  errorCode: number;
  groupId: string;
  state: string;
  protocolType: string;
  protocol: string;
  members: DescribeGroupsResponseV4Member[];
  authorizedOperations: number;
}

export interface DescribeGroupsResponseV4Body {
  throttleTime: number;
  clientSideThrottleTime: number;
  groups: DescribeGroupsResponseV4Group[];
}

/**
 * DescribeGroups Response (Version: 4) => throttle_time_ms [groups]
 *   members => member_id group_instance_id client_id client_host member_metadata member_assignment
 *     group_instance_id => NULLABLE_STRING
 *
 * Adds `groupInstanceId` on each member. Quota timing follows v2 (KIP-219).
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const memberSchema = object([
  field('memberId', string),
  field('groupInstanceId', nullableString),
  field('clientId', string),
  field('clientHost', string),
  field('memberMetadata', bytes),
  field('memberAssignment', bytes),
]);
const groupSchema = object([
  field('errorCode', int16),
  field('groupId', string),
  field('state', string),
  field('protocolType', string),
  field('protocol', string),
  field('members', array(memberSchema)),
  field('authorizedOperations', int32),
]);
const bodySchema = object([field('throttleTime', int32), field('groups', array(groupSchema))]);

export const describeGroupsResponseV4: ResponseDefinition<DescribeGroupsResponseV4Body> = {
  decode: async (rawData) => {
    const decoded = bodySchema.read(new Decoder(rawData));
    return { ...decoded, throttleTime: 0, clientSideThrottleTime: decoded.throttleTime };
  },
  parse: async (data) => {
    await describeGroupsResponseV3.parse(data);
    return data;
  },
};
