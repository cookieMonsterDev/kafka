import { assertNoPrefixedAclOnV0 } from '../acl-v0';
import type { ProtocolFactory, RequestFamily } from '../index';
import { createAclsRequestV0 } from './v0/request';
import { createAclsResponseV0 } from './v0/response';
import { createAclsRequestV1 } from './v1/request';
import { createAclsResponseV1 } from './v1/response';

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

const VERSIONS: Readonly<Record<number, ProtocolFactory<CreateAclsOptions>>> = {
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
};

export const CreateAcls: RequestFamily<CreateAclsOptions> = Object.freeze({
  versions: Object.freeze(Object.keys(VERSIONS).map(Number)),
  protocol({ version }: { version: number }) {
    const factory = VERSIONS[version];
    if (!factory) throw new Error(`Invariant violated: no CreateAcls protocol for version ${version}`);
    return factory;
  },
});
