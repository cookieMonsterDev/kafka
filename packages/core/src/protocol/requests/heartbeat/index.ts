import { defineRequestFamily, type VersionFactories } from '../index';
import { KAFKA_APIS } from '../kafka-apis';
import { heartbeatRequestV0 } from './v0/request';
import { heartbeatResponseV0 } from './v0/response';
import { heartbeatRequestV1 } from './v1/request';
import { heartbeatResponseV1 } from './v1/response';
import { heartbeatRequestV2 } from './v2/request';
import { heartbeatResponseV2 } from './v2/response';
import { heartbeatRequestV3 } from './v3/request';
import { heartbeatResponseV3 } from './v3/response';
import { heartbeatRequestV4 } from './v4/request';
import { heartbeatResponseV4 } from './v4/response';

export interface HeartbeatOptions {
  groupId: string;
  groupGenerationId: number;
  memberId: string;
  groupInstanceId?: string | null;
}

const VERSIONS = {
  0: (options) => ({ request: heartbeatRequestV0(options), response: heartbeatResponseV0 }),
  1: (options) => ({ request: heartbeatRequestV1(options), response: heartbeatResponseV1 }),
  2: (options) => ({ request: heartbeatRequestV2(options), response: heartbeatResponseV2 }),
  3: (options) => ({
    request: heartbeatRequestV3({ ...options, groupInstanceId: options.groupInstanceId ?? null }),
    response: heartbeatResponseV3,
  }),
  4: (options) => ({
    request: heartbeatRequestV4({ ...options, groupInstanceId: options.groupInstanceId ?? null }),
    response: heartbeatResponseV4,
  }),
} satisfies VersionFactories<HeartbeatOptions>;

export const Heartbeat = defineRequestFamily(KAFKA_APIS.Heartbeat, VERSIONS);
