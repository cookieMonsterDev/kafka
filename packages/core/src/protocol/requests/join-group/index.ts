import type { ProtocolFactory, RequestFamily } from '../index';
import { type GroupProtocol, joinGroupRequestV0, withDefaultMetadata } from './v0/request';
import { joinGroupResponseV0 } from './v0/response';
import { joinGroupRequestV1 } from './v1/request';
import { joinGroupResponseV1 } from './v1/response';
import { joinGroupRequestV2 } from './v2/request';
import { joinGroupResponseV2 } from './v2/response';
import { joinGroupRequestV3 } from './v3/request';
import { joinGroupResponseV3 } from './v3/response';
import { joinGroupRequestV4 } from './v4/request';
import { joinGroupResponseV4 } from './v4/response';
import { joinGroupRequestV5 } from './v5/request';
import { joinGroupResponseV5 } from './v5/response';

export interface JoinGroupOptions {
  groupId: string;
  sessionTimeout: number;
  rebalanceTimeout?: number;
  memberId: string;
  groupInstanceId?: string | null;
  protocolType: string;
  groupProtocols: GroupProtocol[];
}

/**
 * JoinGroup can block up to `sessionTimeout` (or `rebalanceTimeout` from v1). Override the
 * connection timeout so a long rebalance is not killed as a request timeout.
 * @see https://github.com/apache/kafka/pull/5203
 */
const NETWORK_DELAY = 5000;

function joinGroupRequestTimeout({
  rebalanceTimeout,
  sessionTimeout,
}: {
  rebalanceTimeout?: number;
  sessionTimeout: number;
}): number {
  const timeout = rebalanceTimeout || sessionTimeout;
  return Number.isSafeInteger(timeout + NETWORK_DELAY) ? timeout + NETWORK_DELAY : timeout;
}

function joinGroupLogResponseError(memberId: string | undefined): boolean {
  return memberId != null && memberId !== '';
}

const VERSIONS: Readonly<Record<number, ProtocolFactory<JoinGroupOptions>>> = {
  0: (options) => ({
    request: joinGroupRequestV0({ ...options, groupProtocols: withDefaultMetadata(options.groupProtocols) }),
    response: joinGroupResponseV0,
    requestTimeout: joinGroupRequestTimeout({ sessionTimeout: options.sessionTimeout }),
  }),
  1: (options) => ({
    request: joinGroupRequestV1({
      ...options,
      rebalanceTimeout: options.rebalanceTimeout ?? options.sessionTimeout,
      groupProtocols: withDefaultMetadata(options.groupProtocols),
    }),
    response: joinGroupResponseV1,
    requestTimeout: joinGroupRequestTimeout(options),
  }),
  2: (options) => ({
    request: joinGroupRequestV2({
      ...options,
      rebalanceTimeout: options.rebalanceTimeout ?? options.sessionTimeout,
      groupProtocols: withDefaultMetadata(options.groupProtocols),
    }),
    response: joinGroupResponseV2,
    requestTimeout: joinGroupRequestTimeout(options),
  }),
  3: (options) => ({
    request: joinGroupRequestV3({
      ...options,
      rebalanceTimeout: options.rebalanceTimeout ?? options.sessionTimeout,
      groupProtocols: withDefaultMetadata(options.groupProtocols),
    }),
    response: joinGroupResponseV3,
    requestTimeout: joinGroupRequestTimeout(options),
  }),
  4: (options) => ({
    request: joinGroupRequestV4({
      ...options,
      rebalanceTimeout: options.rebalanceTimeout ?? options.sessionTimeout,
      groupProtocols: withDefaultMetadata(options.groupProtocols),
    }),
    response: joinGroupResponseV4,
    requestTimeout: joinGroupRequestTimeout(options),
    logResponseError: joinGroupLogResponseError(options.memberId),
  }),
  5: (options) => ({
    request: joinGroupRequestV5({
      ...options,
      rebalanceTimeout: options.rebalanceTimeout ?? options.sessionTimeout,
      groupInstanceId: options.groupInstanceId ?? null,
      groupProtocols: withDefaultMetadata(options.groupProtocols),
    }),
    response: joinGroupResponseV5,
    requestTimeout: joinGroupRequestTimeout(options),
    logResponseError: joinGroupLogResponseError(options.memberId),
  }),
};

export const JoinGroup: RequestFamily<JoinGroupOptions> = Object.freeze({
  versions: Object.freeze(Object.keys(VERSIONS).map(Number)),
  protocol({ version }: { version: number }) {
    const factory = VERSIONS[version];
    if (!factory) throw new Error(`Invariant violated: no JoinGroup protocol for version ${version}`);
    return factory;
  },
});
