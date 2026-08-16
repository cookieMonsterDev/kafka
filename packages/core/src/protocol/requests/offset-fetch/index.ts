import type { ProtocolFactory, RequestFamily } from '../index';
import type { OffsetFetchTopicOptions } from './shared';
import { offsetFetchRequestV1 } from './v1/request';
import { offsetFetchResponseV1 } from './v1/response';
import { offsetFetchRequestV2 } from './v2/request';
import { offsetFetchResponseV2 } from './v2/response';
import { offsetFetchRequestV3 } from './v3/request';
import { offsetFetchResponseV3 } from './v3/response';
import { offsetFetchRequestV4 } from './v4/request';
import { offsetFetchResponseV4 } from './v4/response';
import { offsetFetchRequestV5 } from './v5/request';
import { offsetFetchResponseV5 } from './v5/response';
import { offsetFetchRequestV6 } from './v6/request';
import { offsetFetchResponseV6 } from './v6/response';
import { offsetFetchRequestV7 } from './v7/request';
import { offsetFetchResponseV7 } from './v7/response';

export interface OffsetFetchOptions {
  groupId: string;
  topics: OffsetFetchTopicOptions[];
  requireStable?: boolean;
}

const VERSIONS: Readonly<Record<number, ProtocolFactory<OffsetFetchOptions>>> = {
  1: ({ groupId, topics }) => ({ request: offsetFetchRequestV1({ groupId, topics }), response: offsetFetchResponseV1 }),
  2: ({ groupId, topics }) => ({ request: offsetFetchRequestV2({ groupId, topics }), response: offsetFetchResponseV2 }),
  3: ({ groupId, topics }) => ({ request: offsetFetchRequestV3({ groupId, topics }), response: offsetFetchResponseV3 }),
  4: ({ groupId, topics }) => ({ request: offsetFetchRequestV4({ groupId, topics }), response: offsetFetchResponseV4 }),
  5: ({ groupId, topics }) => ({ request: offsetFetchRequestV5({ groupId, topics }), response: offsetFetchResponseV5 }),
  6: ({ groupId, topics }) => ({ request: offsetFetchRequestV6({ groupId, topics }), response: offsetFetchResponseV6 }),
  7: ({ groupId, topics, requireStable = false }) => ({
    request: offsetFetchRequestV7({ groupId, topics, requireStable }),
    response: offsetFetchResponseV7,
  }),
};

export const OffsetFetch: RequestFamily<OffsetFetchOptions> = Object.freeze({
  versions: Object.freeze(Object.keys(VERSIONS).map(Number)),
  protocol({ version }: { version: number }) {
    const factory = VERSIONS[version];
    if (!factory) throw new Error(`Invariant violated: no OffsetFetch protocol for version ${version}`);
    return factory;
  },
});
