import { KafkaNonRetriableError } from '../../../errors';
import { defineRequestFamily, type VersionFactories } from '../index';
import { KAFKA_APIS } from '../kafka-apis';
import { listGroupsRequestV0 } from './v0/request';
import { listGroupsResponseV0 } from './v0/response';
import { listGroupsRequestV1 } from './v1/request';
import { listGroupsResponseV1 } from './v1/response';
import { listGroupsRequestV2 } from './v2/request';
import { listGroupsResponseV2 } from './v2/response';
import { listGroupsRequestV3 } from './v3/request';
import { listGroupsResponseV3 } from './v3/response';
import { listGroupsRequestV4 } from './v4/request';
import { listGroupsResponseV4 } from './v4/response';
import { listGroupsRequestV5 } from './v5/request';
import { listGroupsResponseV5 } from './v5/response';

export interface ListGroupsOptions {
  states?: string[];
  types?: string[];
}

/** Below v5 the broker cannot filter by type, so a type filter throws instead of listing every group. */
function withoutTypesFilter<T>(version: number, build: (options: ListGroupsOptions) => T) {
  return (options: ListGroupsOptions): T => {
    if (options.types !== undefined && options.types.length > 0) {
      throw new KafkaNonRetriableError(
        `ListGroups v${version} cannot filter groups by type; this broker needs ListGroups v5 or newer`,
      );
    }
    return build(options);
  };
}

const VERSIONS = {
  0: withoutTypesFilter(0, () => ({ request: listGroupsRequestV0({}), response: listGroupsResponseV0 })),
  1: withoutTypesFilter(1, () => ({ request: listGroupsRequestV1({}), response: listGroupsResponseV1 })),
  2: withoutTypesFilter(2, () => ({ request: listGroupsRequestV2({}), response: listGroupsResponseV2 })),
  3: withoutTypesFilter(3, () => ({ request: listGroupsRequestV3({}), response: listGroupsResponseV3 })),
  4: withoutTypesFilter(4, (options) => ({
    request: listGroupsRequestV4({ states: options.states ?? [] }),
    response: listGroupsResponseV4,
  })),
  5: (options) => ({
    request: listGroupsRequestV5({ states: options.states ?? [], types: options.types ?? [] }),
    response: listGroupsResponseV5,
  }),
} satisfies VersionFactories<ListGroupsOptions>;

export const ListGroups = defineRequestFamily(KAFKA_APIS.ListGroups, VERSIONS);
