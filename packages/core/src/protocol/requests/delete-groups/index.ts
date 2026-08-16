import type { ProtocolFactory, RequestFamily } from '../index';
import { deleteGroupsRequestV0 } from './v0/request';
import { deleteGroupsResponseV0 } from './v0/response';
import { deleteGroupsRequestV1 } from './v1/request';
import { deleteGroupsResponseV1 } from './v1/response';

export interface DeleteGroupsOptions {
  groupIds: string[];
}

const VERSIONS: Readonly<Record<number, ProtocolFactory<DeleteGroupsOptions>>> = {
  0: (options) => ({ request: deleteGroupsRequestV0(options), response: deleteGroupsResponseV0 }),
  1: (options) => ({ request: deleteGroupsRequestV1(options), response: deleteGroupsResponseV1 }),
};

export const DeleteGroups: RequestFamily<DeleteGroupsOptions> = Object.freeze({
  versions: Object.freeze(Object.keys(VERSIONS).map(Number)),
  protocol({ version }: { version: number }) {
    const factory = VERSIONS[version];
    if (!factory) throw new Error(`Invariant violated: no DeleteGroups protocol for version ${version}`);
    return factory;
  },
});
