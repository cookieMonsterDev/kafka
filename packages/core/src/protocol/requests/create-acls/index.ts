import type { ProtocolFactory, RequestFamily } from '../index.js';
import { createAclsRequestV1 } from './v1/request.js';
import { createAclsResponseV1 } from './v1/response.js';

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
