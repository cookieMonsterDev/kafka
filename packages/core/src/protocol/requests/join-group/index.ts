import type { ProtocolFactory, RequestFamily } from '../index.js';
import { type GroupProtocol, joinGroupRequestV0, withDefaultMetadata } from './v0/request.js';
import { joinGroupResponseV0 } from './v0/response.js';
import { joinGroupRequestV1 } from './v1/request.js';
import { joinGroupResponseV1 } from './v1/response.js';
import { joinGroupRequestV2 } from './v2/request.js';
import { joinGroupResponseV2 } from './v2/response.js';
import { joinGroupRequestV3 } from './v3/request.js';
import { joinGroupResponseV3 } from './v3/response.js';
import { joinGroupRequestV4 } from './v4/request.js';
import { joinGroupResponseV4 } from './v4/response.js';
import { joinGroupRequestV5 } from './v5/request.js';
import { joinGroupResponseV5 } from './v5/response.js';

export interface JoinGroupOptions {
  groupId: string;
  sessionTimeout: number;
  rebalanceTimeout?: number;
  memberId: string;
  groupInstanceId?: string | null;
  protocolType: string;
  groupProtocols: GroupProtocol[];
}

const VERSIONS: Readonly<Record<number, ProtocolFactory<JoinGroupOptions>>> = {
  0: (options) => ({
    request: joinGroupRequestV0({ ...options, groupProtocols: withDefaultMetadata(options.groupProtocols) }),
    response: joinGroupResponseV0,
  }),
  1: (options) => ({
    request: joinGroupRequestV1({
      ...options,
      rebalanceTimeout: options.rebalanceTimeout ?? options.sessionTimeout,
      groupProtocols: withDefaultMetadata(options.groupProtocols),
    }),
    response: joinGroupResponseV1,
  }),
  2: (options) => ({
    request: joinGroupRequestV2({
      ...options,
      rebalanceTimeout: options.rebalanceTimeout ?? options.sessionTimeout,
      groupProtocols: withDefaultMetadata(options.groupProtocols),
    }),
    response: joinGroupResponseV2,
  }),
  3: (options) => ({
    request: joinGroupRequestV3({
      ...options,
      rebalanceTimeout: options.rebalanceTimeout ?? options.sessionTimeout,
      groupProtocols: withDefaultMetadata(options.groupProtocols),
    }),
    response: joinGroupResponseV3,
  }),
  4: (options) => ({
    request: joinGroupRequestV4({
      ...options,
      rebalanceTimeout: options.rebalanceTimeout ?? options.sessionTimeout,
      groupProtocols: withDefaultMetadata(options.groupProtocols),
    }),
    response: joinGroupResponseV4,
  }),
  5: (options) => ({
    request: joinGroupRequestV5({
      ...options,
      rebalanceTimeout: options.rebalanceTimeout ?? options.sessionTimeout,
      groupInstanceId: options.groupInstanceId ?? null,
      groupProtocols: withDefaultMetadata(options.groupProtocols),
    }),
    response: joinGroupResponseV5,
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
