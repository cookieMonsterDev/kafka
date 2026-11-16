import { assertNoPrefixedAclOnV0 } from '../acl-v0';
import { defineRequestFamily, type VersionFactories } from '../index';
import { KAFKA_APIS } from '../kafka-apis';
import { createAclsRequestV0 } from './v0/request';
import { createAclsResponseV0 } from './v0/response';
import { createAclsRequestV1 } from './v1/request';
import { createAclsResponseV1 } from './v1/response';
import { createAclsRequestV2 } from './v2/request';
import { createAclsResponseV2 } from './v2/response';
import { createAclsRequestV3 } from './v3/request';
import { createAclsResponseV3 } from './v3/response';

export interface CreateAclsOptions {
  creations: {
    resourceType: number;
    resourceName: string;
    resourcePatternType: number;
    principal: string;
    host: string;
    operation: number;
    permissionType: number;
  }[];
}

const VERSIONS = {
  0: (values) => {
    for (const creation of values.creations) {
      assertNoPrefixedAclOnV0(creation.resourcePatternType);
    }
    return {
      request: createAclsRequestV0({
        creations: values.creations.map(
          ({ resourceType, resourceName, principal, host, operation, permissionType }) => ({
            resourceType,
            resourceName,
            principal,
            host,
            operation,
            permissionType,
          }),
        ),
      }),
      response: createAclsResponseV0,
    };
  },
  1: (values) => ({ request: createAclsRequestV1(values), response: createAclsResponseV1 }),
  2: (values) => ({ request: createAclsRequestV2(values), response: createAclsResponseV2 }),
  3: (values) => ({ request: createAclsRequestV3(values), response: createAclsResponseV3 }),
} satisfies VersionFactories<CreateAclsOptions>;

export const CreateAcls = defineRequestFamily(KAFKA_APIS.CreateAcls, VERSIONS);
