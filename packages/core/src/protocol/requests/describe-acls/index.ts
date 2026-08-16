import { assertNoPrefixedAclOnV0 } from '../acl-v0';
import type { ProtocolFactory, RequestFamily } from '../index';
import { describeAclsRequestV0 } from './v0/request';
import { describeAclsResponseV0 } from './v0/response';
import { describeAclsRequestV1 } from './v1/request';
import { describeAclsResponseV1 } from './v1/response';
import { describeAclsRequestV2 } from './v2/request';
import { describeAclsResponseV2 } from './v2/response';
import { describeAclsRequestV3 } from './v3/request';
import { describeAclsResponseV3 } from './v3/response';

export interface DescribeAclsOptions {
  resourceType: number;
  resourceName: string | null;
  resourcePatternType: number;
  principal: string | null;
  host: string | null;
  operation: number;
  permissionType: number;
}

const VERSIONS: Readonly<Record<number, ProtocolFactory<DescribeAclsOptions>>> = {
  0: (values) => {
    assertNoPrefixedAclOnV0(values.resourcePatternType);
    return {
      request: describeAclsRequestV0({
        resourceType: values.resourceType,
        resourceName: values.resourceName,
        principal: values.principal,
        host: values.host,
        operation: values.operation,
        permissionType: values.permissionType,
      }),
      response: describeAclsResponseV0,
    };
  },
  1: (values) => ({ request: describeAclsRequestV1(values), response: describeAclsResponseV1 }),
  2: (values) => ({ request: describeAclsRequestV2(values), response: describeAclsResponseV2 }),
  3: (values) => ({ request: describeAclsRequestV3(values), response: describeAclsResponseV3 }),
};

export const DescribeAcls: RequestFamily<DescribeAclsOptions> = Object.freeze({
  versions: Object.freeze(Object.keys(VERSIONS).map(Number)),
  protocol({ version }: { version: number }) {
    const factory = VERSIONS[version];
    if (!factory) throw new Error(`Invariant violated: no DescribeAcls protocol for version ${version}`);
    return factory;
  },
});
