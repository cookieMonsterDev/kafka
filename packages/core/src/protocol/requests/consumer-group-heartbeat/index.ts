import type { ProtocolFactory, RequestFamily } from '../index';
import { consumerGroupHeartbeatRequestV0 } from './v0/request';
import { consumerGroupHeartbeatResponseV0 } from './v0/response';
import { consumerGroupHeartbeatRequestV1 } from './v1/request';
import { consumerGroupHeartbeatResponseV1 } from './v1/response';
import type { ConsumerGroupHeartbeatTopicPartitions } from './shared';

export type { ConsumerGroupHeartbeatTopicPartitions } from './shared';
export type { ConsumerGroupHeartbeatResponseV0Body } from './v0/response';
export type { ConsumerGroupHeartbeatResponseV1Body } from './v1/response';

/** Member epoch 0 joins the group (KIP-848). */
export const CONSUMER_GROUP_JOIN_EPOCH = 0;
/** Member epoch -1 leaves the group (KIP-848). */
export const CONSUMER_GROUP_LEAVE_EPOCH = -1;
/** Member epoch -2 is a static-member rejoin (KIP-848). */
export const CONSUMER_GROUP_STATIC_LEAVE_EPOCH = -2;

export interface ConsumerGroupHeartbeatOptions {
  groupId: string;
  memberId: string;
  memberEpoch: number;
  instanceId?: string | null;
  rackId?: string | null;
  rebalanceTimeoutMs?: number;
  subscribedTopicNames?: string[] | null;
  subscribedTopicRegex?: string | null;
  serverAssignor?: string | null;
  topicPartitions?: ConsumerGroupHeartbeatTopicPartitions[] | null;
}

const v0Fields = (options: ConsumerGroupHeartbeatOptions) => ({
  groupId: options.groupId,
  memberId: options.memberId,
  memberEpoch: options.memberEpoch,
  instanceId: options.instanceId ?? null,
  rackId: options.rackId ?? null,
  rebalanceTimeoutMs: options.rebalanceTimeoutMs ?? -1,
  subscribedTopicNames: options.subscribedTopicNames ?? null,
  serverAssignor: options.serverAssignor ?? null,
  topicPartitions: options.topicPartitions ?? null,
});

const VERSIONS: Readonly<Record<number, ProtocolFactory<ConsumerGroupHeartbeatOptions>>> = {
  0: (options) => ({
    request: consumerGroupHeartbeatRequestV0(v0Fields(options)),
    response: consumerGroupHeartbeatResponseV0,
  }),
  1: (options) => ({
    request: consumerGroupHeartbeatRequestV1({
      ...v0Fields(options),
      subscribedTopicRegex: options.subscribedTopicRegex ?? null,
    }),
    response: consumerGroupHeartbeatResponseV1,
  }),
};

export const ConsumerGroupHeartbeat: RequestFamily<ConsumerGroupHeartbeatOptions> = Object.freeze({
  versions: Object.freeze(Object.keys(VERSIONS).map(Number)),
  protocol({ version }: { version: number }) {
    const factory = VERSIONS[version];
    if (!factory) throw new Error(`Invariant violated: no ConsumerGroupHeartbeat protocol for version ${version}`);
    return factory;
  },
});
