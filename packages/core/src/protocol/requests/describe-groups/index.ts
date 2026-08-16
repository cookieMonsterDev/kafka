import type { ProtocolFactory, RequestFamily } from '../index';
import { describeGroupsRequestV0 } from './v0/request';
import { describeGroupsResponseV0 } from './v0/response';
import { describeGroupsRequestV1 } from './v1/request';
import { describeGroupsResponseV1 } from './v1/response';
import { describeGroupsRequestV2 } from './v2/request';
import { describeGroupsResponseV2 } from './v2/response';

export interface DescribeGroupsOptions {
  groupIds: string[];
}

const VERSIONS: Readonly<Record<number, ProtocolFactory<DescribeGroupsOptions>>> = {
  0: (options) => ({ request: describeGroupsRequestV0(options), response: describeGroupsResponseV0 }),
  1: (options) => ({ request: describeGroupsRequestV1(options), response: describeGroupsResponseV1 }),
  2: (options) => ({ request: describeGroupsRequestV2(options), response: describeGroupsResponseV2 }),
};

export const DescribeGroups: RequestFamily<DescribeGroupsOptions> = Object.freeze({
  versions: Object.freeze(Object.keys(VERSIONS).map(Number)),
  protocol({ version }: { version: number }) {
    const factory = VERSIONS[version];
    if (!factory) throw new Error(`Invariant violated: no DescribeGroups protocol for version ${version}`);
    return factory;
  },
});
