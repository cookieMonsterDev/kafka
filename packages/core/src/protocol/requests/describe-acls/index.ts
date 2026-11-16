import { assertNoPrefixedAclOnV0 } from '../acl-v0';
import { defineRequestFamily, type VersionFactories } from '../index';
import { KAFKA_APIS } from '../kafka-apis';
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

const VERSIONS = {
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
} satisfies VersionFactories<DescribeAclsOptions>;

export const DescribeAcls = defineRequestFamily(KAFKA_APIS.DescribeAcls, VERSIONS);
