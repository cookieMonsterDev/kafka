import { Decoder } from '../../../decoder';
import { array, bytes, field, int16, int32, object, string } from '../../../schema';
import type { ResponseDefinition } from '../../../schema';
import { describeGroupsResponseV2 } from '../v2/response';

export interface DescribeGroupsResponseV3Member {
  memberId: string;
  clientId: string;
  clientHost: string;
  memberMetadata: Buffer;
  memberAssignment: Buffer;
}

export interface DescribeGroupsResponseV3Group {
  errorCode: number;
  groupId: string;
  state: string;
  protocolType: string;
  protocol: string;
  members: DescribeGroupsResponseV3Member[];
  authorizedOperations: number;
}

export interface DescribeGroupsResponseV3Body {
  throttleTime: number;
  clientSideThrottleTime: number;
  groups: DescribeGroupsResponseV3Group[];
}

/**
 * DescribeGroups Response (Version: 3) => throttle_time_ms [groups]
 *   groups => error_code group_id state protocol_type protocol [members] authorized_operations
 *     authorized_operations => INT32
 *
 * Adds `authorizedOperations` on each group (KIP-430). Quota timing follows v2 (KIP-219).
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const memberSchema = object([
  field('memberId', string),
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

export const describeGroupsResponseV3: ResponseDefinition<DescribeGroupsResponseV3Body> = {
  decode: async (rawData) => {
    const decoded = bodySchema.read(new Decoder(rawData));
    return { ...decoded, throttleTime: 0, clientSideThrottleTime: decoded.throttleTime };
  },
  parse: async (data) => {
    await describeGroupsResponseV2.parse(data);
    return data;
  },
};
