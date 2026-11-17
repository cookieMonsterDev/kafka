import { defineRequestFamily, type VersionFactories } from '../index';
import { KAFKA_APIS } from '../kafka-apis';
import { deleteGroupsRequestV0 } from './v0/request';
import { deleteGroupsResponseV0 } from './v0/response';
import { deleteGroupsRequestV1 } from './v1/request';
import { deleteGroupsResponseV1 } from './v1/response';
import { deleteGroupsRequestV2 } from './v2/request';
import { deleteGroupsResponseV2 } from './v2/response';

export interface DeleteGroupsOptions {
  groupIds: string[];
}

const VERSIONS = {
  0: (options) => ({ request: deleteGroupsRequestV0(options), response: deleteGroupsResponseV0 }),
  1: (options) => ({ request: deleteGroupsRequestV1(options), response: deleteGroupsResponseV1 }),
  2: (options) => ({ request: deleteGroupsRequestV2(options), response: deleteGroupsResponseV2 }),
} satisfies VersionFactories<DeleteGroupsOptions>;

export const DeleteGroups = defineRequestFamily(KAFKA_APIS.DeleteGroups, VERSIONS);
