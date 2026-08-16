import type { ProtocolFactory, RequestFamily } from '../index';
import { listGroupsRequestV0 } from './v0/request';
import { listGroupsResponseV0 } from './v0/response';
import { listGroupsRequestV1 } from './v1/request';
import { listGroupsResponseV1 } from './v1/response';
import { listGroupsRequestV2 } from './v2/request';
import { listGroupsResponseV2 } from './v2/response';
import { listGroupsRequestV3 } from './v3/request';
import { listGroupsResponseV3 } from './v3/response';
import { listGroupsRequestV4 } from './v4/request';
import { listGroupsResponseV4 } from './v4/response';

export interface ListGroupsOptions {
  states?: string[];
}

const VERSIONS: Readonly<Record<number, ProtocolFactory<ListGroupsOptions>>> = {
  0: () => ({ request: listGroupsRequestV0({}), response: listGroupsResponseV0 }),
  1: () => ({ request: listGroupsRequestV1({}), response: listGroupsResponseV1 }),
  2: () => ({ request: listGroupsRequestV2({}), response: listGroupsResponseV2 }),
  3: () => ({ request: listGroupsRequestV3({}), response: listGroupsResponseV3 }),
  4: (options) => ({
    request: listGroupsRequestV4({ states: options.states ?? [] }),
    response: listGroupsResponseV4,
  }),
};

export const ListGroups: RequestFamily<ListGroupsOptions> = Object.freeze({
  versions: Object.freeze(Object.keys(VERSIONS).map(Number)),
  protocol({ version }: { version: number }) {
    const factory = VERSIONS[version];
    if (!factory) throw new Error(`Invariant violated: no ListGroups protocol for version ${version}`);
    return factory;
  },
});
