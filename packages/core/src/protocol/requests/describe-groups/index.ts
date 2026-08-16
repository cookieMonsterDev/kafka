import type { ProtocolFactory, RequestFamily } from '../index';
import { describeGroupsRequestV0 } from './v0/request';
import { describeGroupsResponseV0 } from './v0/response';
import { describeGroupsRequestV1 } from './v1/request';
import { describeGroupsResponseV1 } from './v1/response';
import { describeGroupsRequestV2 } from './v2/request';
import { describeGroupsResponseV2 } from './v2/response';
import { describeGroupsRequestV3 } from './v3/request';
import { describeGroupsResponseV3 } from './v3/response';
import { describeGroupsRequestV4 } from './v4/request';
import { describeGroupsResponseV4 } from './v4/response';
import { describeGroupsRequestV5 } from './v5/request';
import { describeGroupsResponseV5 } from './v5/response';

export interface DescribeGroupsOptions {
  groupIds: string[];
  includeAuthorizedOperations?: boolean;
}

function withAuthorizedOperations(options: DescribeGroupsOptions) {
  return {
    groupIds: options.groupIds,
    includeAuthorizedOperations: options.includeAuthorizedOperations ?? false,
  };
}

const VERSIONS: Readonly<Record<number, ProtocolFactory<DescribeGroupsOptions>>> = {
  0: (options) => ({ request: describeGroupsRequestV0(options), response: describeGroupsResponseV0 }),
  1: (options) => ({ request: describeGroupsRequestV1(options), response: describeGroupsResponseV1 }),
  2: (options) => ({ request: describeGroupsRequestV2(options), response: describeGroupsResponseV2 }),
  3: (options) => ({
    request: describeGroupsRequestV3(withAuthorizedOperations(options)),
    response: describeGroupsResponseV3,
  }),
  4: (options) => ({
    request: describeGroupsRequestV4(withAuthorizedOperations(options)),
    response: describeGroupsResponseV4,
  }),
  5: (options) => ({
    request: describeGroupsRequestV5(withAuthorizedOperations(options)),
    response: describeGroupsResponseV5,
  }),
};

export const DescribeGroups: RequestFamily<DescribeGroupsOptions> = Object.freeze({
  versions: Object.freeze(Object.keys(VERSIONS).map(Number)),
  protocol({ version }: { version: number }) {
    const factory = VERSIONS[version];
    if (!factory) throw new Error(`Invariant violated: no DescribeGroups protocol for version ${version}`);
    return factory;
  },
});
