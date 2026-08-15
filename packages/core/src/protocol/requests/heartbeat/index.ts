import type { ProtocolFactory, RequestFamily } from '../index.js';
import { heartbeatRequestV0 } from './v0/request.js';
import { heartbeatResponseV0 } from './v0/response.js';
import { heartbeatRequestV1 } from './v1/request.js';
import { heartbeatResponseV1 } from './v1/response.js';
import { heartbeatRequestV2 } from './v2/request.js';
import { heartbeatResponseV2 } from './v2/response.js';
import { heartbeatRequestV3 } from './v3/request.js';
import { heartbeatResponseV3 } from './v3/response.js';

export interface HeartbeatOptions {
  groupId: string;
  groupGenerationId: number;
  memberId: string;
  groupInstanceId?: string | null;
}

const VERSIONS: Readonly<Record<number, ProtocolFactory<HeartbeatOptions>>> = {
  0: (options) => ({ request: heartbeatRequestV0(options), response: heartbeatResponseV0 }),
  1: (options) => ({ request: heartbeatRequestV1(options), response: heartbeatResponseV1 }),
  2: (options) => ({ request: heartbeatRequestV2(options), response: heartbeatResponseV2 }),
  3: (options) => ({
    request: heartbeatRequestV3({ ...options, groupInstanceId: options.groupInstanceId ?? null }),
    response: heartbeatResponseV3,
  }),
};

export const Heartbeat: RequestFamily<HeartbeatOptions> = Object.freeze({
  versions: Object.freeze(Object.keys(VERSIONS).map(Number)),
  protocol({ version }: { version: number }) {
    const factory = VERSIONS[version];
    if (!factory) throw new Error(`Invariant violated: no Heartbeat protocol for version ${version}`);
    return factory;
  },
});
