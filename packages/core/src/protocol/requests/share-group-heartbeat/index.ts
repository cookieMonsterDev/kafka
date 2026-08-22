import type { ProtocolFactory, RequestFamily } from '../index';
import { shareGroupHeartbeatRequestV1 } from './v1/request';
import { shareGroupHeartbeatResponseV1 } from './v1/response';

export type { ShareGroupHeartbeatTopicPartitions } from './shared';
export type { ShareGroupHeartbeatResponseV1Body } from './v1/response';

/** Member epoch 0 joins the group (KIP-932). */
export const SHARE_GROUP_JOIN_EPOCH = 0;
/** Member epoch -1 leaves the group (KIP-932). */
export const SHARE_GROUP_LEAVE_EPOCH = -1;

export interface ShareGroupHeartbeatOptions {
  groupId: string;
  memberId: string;
  memberEpoch: number;
  rackId?: string | null;
  subscribedTopicNames?: string[] | null;
}

const fields = (options: ShareGroupHeartbeatOptions) => ({
  groupId: options.groupId,
  memberId: options.memberId,
  memberEpoch: options.memberEpoch,
  rackId: options.rackId ?? null,
  subscribedTopicNames: options.subscribedTopicNames ?? null,
});

const VERSIONS: Readonly<Record<number, ProtocolFactory<ShareGroupHeartbeatOptions>>> = {
  1: (options) => ({
    request: shareGroupHeartbeatRequestV1(fields(options)),
    response: shareGroupHeartbeatResponseV1,
  }),
};

export const ShareGroupHeartbeat: RequestFamily<ShareGroupHeartbeatOptions> = Object.freeze({
  versions: Object.freeze(Object.keys(VERSIONS).map(Number)),
  protocol({ version }: { version: number }) {
    const factory = VERSIONS[version];
    if (!factory) throw new Error(`Invariant violated: no ShareGroupHeartbeat protocol for version ${version}`);
    return factory;
  },
});
