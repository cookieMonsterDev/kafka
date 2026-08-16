import { assertNoPrefixedAclOnV0 } from '../acl-v0';
import type { ProtocolFactory, RequestFamily } from '../index';
import { deleteAclsRequestV0 } from './v0/request';
import { deleteAclsResponseV0 } from './v0/response';
import { deleteAclsRequestV1 } from './v1/request';
import { deleteAclsResponseV1 } from './v1/response';
import { deleteAclsRequestV2 } from './v2/request';
import { deleteAclsResponseV2 } from './v2/response';
import { deleteAclsRequestV3 } from './v3/request';
import { deleteAclsResponseV3 } from './v3/response';

export interface DeleteAclsOptions {
  filters: {
    resourceType: number;
    resourceName: string | null;
    resourcePatternType: number;
    principal: string | null;
    host: string | null;
    operation: number;
    permissionType: number;
  }[];
}

const VERSIONS: Readonly<Record<number, ProtocolFactory<DeleteAclsOptions>>> = {
  0: (values) => {
    for (const filter of values.filters) {
      assertNoPrefixedAclOnV0(filter.resourcePatternType);
    }
    return {
      request: deleteAclsRequestV0({
        filters: values.filters.map(({ resourceType, resourceName, principal, host, operation, permissionType }) => ({
          resourceType,
          resourceName,
          principal,
          host,
          operation,
          permissionType,
        })),
      }),
      response: deleteAclsResponseV0,
    };
  },
  1: (values) => ({ request: deleteAclsRequestV1(values), response: deleteAclsResponseV1 }),
  2: (values) => ({ request: deleteAclsRequestV2(values), response: deleteAclsResponseV2 }),
  3: (values) => ({ request: deleteAclsRequestV3(values), response: deleteAclsResponseV3 }),
};

export const DeleteAcls: RequestFamily<DeleteAclsOptions> = Object.freeze({
  versions: Object.freeze(Object.keys(VERSIONS).map(Number)),
  protocol({ version }: { version: number }) {
    const factory = VERSIONS[version];
    if (!factory) throw new Error(`Invariant violated: no DeleteAcls protocol for version ${version}`);
    return factory;
  },
});
