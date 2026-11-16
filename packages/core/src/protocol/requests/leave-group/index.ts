import { createErrorFromCode, failIfVersionNotSupported, failure } from '../../error-codes';
import { defineRequestFamily, type VersionFactories, type VersionResponse } from '../index';
import { KAFKA_APIS } from '../kafka-apis';
import { leaveGroupRequestV0 } from './v0/request';
import { leaveGroupResponseV0 } from './v0/response';
import { leaveGroupRequestV1 } from './v1/request';
import { leaveGroupResponseV1 } from './v1/response';
import { leaveGroupRequestV2 } from './v2/request';
import { leaveGroupResponseV2 } from './v2/response';
import { leaveGroupRequestV3 } from './v3/request';
import { leaveGroupResponseV3 } from './v3/response';
import { leaveGroupRequestV4 } from './v4/request';
import { leaveGroupResponseV4 } from './v4/response';
import { leaveGroupRequestV5 } from './v5/request';
import { leaveGroupResponseV5 } from './v5/response';

export interface LeaveGroupMember {
  memberId: string;
  groupInstanceId?: string | null;
  reason?: string | null;
}

export interface LeaveGroupOptions {
  groupId: string;
  memberId?: string;
  groupInstanceId?: string | null;
  members?: LeaveGroupMember[];
}

function toMembers(
  options: LeaveGroupOptions,
): { memberId: string; groupInstanceId: string | null; reason: string | null }[] {
  if (options.members) {
    return options.members.map(({ memberId, groupInstanceId = null, reason = null }) => ({
      memberId,
      groupInstanceId,
      reason,
    }));
  }
  if (options.memberId == null) {
    throw new Error('Invariant violated: LeaveGroup v3+ requires either memberId or members');
  }
  return [{ memberId: options.memberId, groupInstanceId: options.groupInstanceId ?? null, reason: null }];
}

const VERSIONS = {
  0: (options) => {
    if (options.memberId == null) throw new Error('Invariant violated: LeaveGroup v0 requires memberId');
    return {
      request: leaveGroupRequestV0({ groupId: options.groupId, memberId: options.memberId }),
      response: leaveGroupResponseV0,
    };
  },
  1: (options) => {
    if (options.memberId == null) throw new Error('Invariant violated: LeaveGroup v1 requires memberId');
    return {
      request: leaveGroupRequestV1({ groupId: options.groupId, memberId: options.memberId }),
      response: leaveGroupResponseV1,
    };
  },
  2: (options) => {
    if (options.memberId == null) throw new Error('Invariant violated: LeaveGroup v2 requires memberId');
    return {
      request: leaveGroupRequestV2({ groupId: options.groupId, memberId: options.memberId }),
      response: leaveGroupResponseV2,
    };
  },
  3: (options) => ({
    request: leaveGroupRequestV3({ groupId: options.groupId, members: toMembers(options) }),
    response: leaveGroupResponseV3,
  }),
  4: (options) => ({
    request: leaveGroupRequestV4({ groupId: options.groupId, members: toMembers(options) }),
    response: leaveGroupResponseV4,
  }),
  5: (options) => ({
    request: leaveGroupRequestV5({ groupId: options.groupId, members: toMembers(options) }),
    response: leaveGroupResponseV5,
  }),
} satisfies VersionFactories<LeaveGroupOptions>;

export const LeaveGroup = defineRequestFamily(KAFKA_APIS.LeaveGroup, VERSIONS);

/**
 * LeaveGroup that returns each member's error code instead of throwing on the first failed member;
 * only a group-level error throws.
 */
export const LeaveGroupMembers = defineRequestFamily(KAFKA_APIS.LeaveGroup, VERSIONS, {
  parseResponse: (body) => {
    const response = body as VersionResponse<typeof VERSIONS>;
    failIfVersionNotSupported(response.errorCode);
    if (failure(response.errorCode)) throw createErrorFromCode(response.errorCode);
    // Only v3+ (KIP-345) removes several members at once and reports each one.
    return { ...response, members: 'members' in response ? response.members : [] };
  },
});
