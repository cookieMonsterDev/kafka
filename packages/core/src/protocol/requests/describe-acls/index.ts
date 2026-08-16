import type { ProtocolFactory, RequestFamily } from '../index';
import { describeAclsRequestV1 } from './v1/request';
import { describeAclsResponseV1 } from './v1/response';

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
  1: (values) => ({ request: describeAclsRequestV1(values), response: describeAclsResponseV1 }),
};

export const DescribeAcls: RequestFamily<DescribeAclsOptions> = Object.freeze({
  versions: Object.freeze(Object.keys(VERSIONS).map(Number)),
  protocol({ version }: { version: number }) {
    const factory = VERSIONS[version];
    if (!factory) throw new Error(`Invariant violated: no DescribeAcls protocol for version ${version}`);
    return factory;
  },
});
