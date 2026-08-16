import type { ProtocolFactory, RequestFamily } from '../index';
import { leaveGroupRequestV0 } from './v0/request';
import { leaveGroupResponseV0 } from './v0/response';
import { leaveGroupRequestV1 } from './v1/request';
import { leaveGroupResponseV1 } from './v1/response';
import { leaveGroupRequestV2 } from './v2/request';
import { leaveGroupResponseV2 } from './v2/response';
import { leaveGroupRequestV3 } from './v3/request';
import { leaveGroupResponseV3 } from './v3/response';

export interface LeaveGroupMember {
  memberId: string;
  groupInstanceId?: string | null;
}

export interface LeaveGroupOptions {
  groupId: string;
  memberId?: string;
  groupInstanceId?: string | null;
  members?: LeaveGroupMember[];
}

function toMembers(options: LeaveGroupOptions): { memberId: string; groupInstanceId: string | null }[] {
  if (options.members) {
    return options.members.map(({ memberId, groupInstanceId = null }) => ({ memberId, groupInstanceId }));
  }
  if (options.memberId == null) {
    throw new Error('Invariant violated: LeaveGroup v3 requires either memberId or members');
  }
  return [{ memberId: options.memberId, groupInstanceId: options.groupInstanceId ?? null }];
}

const VERSIONS: Readonly<Record<number, ProtocolFactory<LeaveGroupOptions>>> = {
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
};

export const LeaveGroup: RequestFamily<LeaveGroupOptions> = Object.freeze({
  versions: Object.freeze(Object.keys(VERSIONS).map(Number)),
  protocol({ version }: { version: number }) {
    const factory = VERSIONS[version];
    if (!factory) throw new Error(`Invariant violated: no LeaveGroup protocol for version ${version}`);
    return factory;
  },
});
