import type { ProtocolFactory, RequestFamily } from '../index.js';
import { deleteAclsRequestV1 } from './v1/request.js';
import { deleteAclsResponseV1 } from './v1/response.js';

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
  1: (values) => ({ request: deleteAclsRequestV1(values), response: deleteAclsResponseV1 }),
};

export const DeleteAcls: RequestFamily<DeleteAclsOptions> = Object.freeze({
  versions: Object.freeze(Object.keys(VERSIONS).map(Number)),
  protocol({ version }: { version: number }) {
    const factory = VERSIONS[version];
    if (!factory) throw new Error(`Invariant violated: no DeleteAcls protocol for version ${version}`);
    return factory;
  },
});
