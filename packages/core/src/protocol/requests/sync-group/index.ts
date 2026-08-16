import type { ProtocolFactory, RequestFamily } from '../index';
import { syncGroupRequestV0 } from './v0/request';
import { syncGroupResponseV0 } from './v0/response';
import { syncGroupRequestV1 } from './v1/request';
import { syncGroupResponseV1 } from './v1/response';
import { syncGroupRequestV2 } from './v2/request';
import { syncGroupResponseV2 } from './v2/response';
import { syncGroupRequestV3 } from './v3/request';
import { syncGroupResponseV3 } from './v3/response';
import { syncGroupRequestV4 } from './v4/request';
import { syncGroupResponseV4 } from './v4/response';
import { syncGroupRequestV5 } from './v5/request';
import { syncGroupResponseV5 } from './v5/response';

export interface SyncGroupAssignment {
  memberId: string;
  memberAssignment: Buffer;
}

export interface SyncGroupOptions {
  groupId: string;
  generationId: number;
  memberId: string;
  groupInstanceId?: string | null;
  protocolType?: string | null;
  protocolName?: string | null;
  groupAssignment: SyncGroupAssignment[];
}

const VERSIONS: Readonly<Record<number, ProtocolFactory<SyncGroupOptions>>> = {
  0: (options) => ({ request: syncGroupRequestV0(options), response: syncGroupResponseV0 }),
  1: (options) => ({ request: syncGroupRequestV1(options), response: syncGroupResponseV1 }),
  2: (options) => ({ request: syncGroupRequestV2(options), response: syncGroupResponseV2 }),
  3: (options) => ({
    request: syncGroupRequestV3({ ...options, groupInstanceId: options.groupInstanceId ?? null }),
    response: syncGroupResponseV3,
  }),
  4: (options) => ({
    request: syncGroupRequestV4({ ...options, groupInstanceId: options.groupInstanceId ?? null }),
    response: syncGroupResponseV4,
  }),
  5: (options) => ({
    request: syncGroupRequestV5({
      ...options,
      groupInstanceId: options.groupInstanceId ?? null,
      protocolType: options.protocolType ?? null,
      protocolName: options.protocolName ?? null,
    }),
    response: syncGroupResponseV5,
  }),
};

export const SyncGroup: RequestFamily<SyncGroupOptions> = Object.freeze({
  versions: Object.freeze(Object.keys(VERSIONS).map(Number)),
  protocol({ version }: { version: number }) {
    const factory = VERSIONS[version];
    if (!factory) throw new Error(`Invariant violated: no SyncGroup protocol for version ${version}`);
    return factory;
  },
});
