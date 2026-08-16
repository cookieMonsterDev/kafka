import type { ProtocolFactory, RequestFamily } from '../index';
import { syncGroupRequestV0 } from './v0/request';
import { syncGroupResponseV0 } from './v0/response';
import { syncGroupRequestV1 } from './v1/request';
import { syncGroupResponseV1 } from './v1/response';
import { syncGroupRequestV2 } from './v2/request';
import { syncGroupResponseV2 } from './v2/response';
import { syncGroupRequestV3 } from './v3/request';
import { syncGroupResponseV3 } from './v3/response';

export interface SyncGroupAssignment {
  memberId: string;
  memberAssignment: Buffer;
}

export interface SyncGroupOptions {
  groupId: string;
  generationId: number;
  memberId: string;
  groupInstanceId?: string | null;
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
};

export const SyncGroup: RequestFamily<SyncGroupOptions> = Object.freeze({
  versions: Object.freeze(Object.keys(VERSIONS).map(Number)),
  protocol({ version }: { version: number }) {
    const factory = VERSIONS[version];
    if (!factory) throw new Error(`Invariant violated: no SyncGroup protocol for version ${version}`);
    return factory;
  },
});
