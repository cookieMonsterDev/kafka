import { KafkaNonRetriableError } from '../../../errors';
import { defineRequestFamily, type VersionFactories } from '../index';
import { KAFKA_APIS } from '../kafka-apis';
import { listConfigResourcesRequestV0 } from './v0/request';
import { listConfigResourcesResponseV0 } from './v0/response';
import { listConfigResourcesRequestV1 } from './v1/request';
import { listConfigResourcesResponseV1 } from './v1/response';

export interface ListConfigResourcesOptions {
  resourceTypes?: number[];
}

const VERSIONS = {
  0: (options) => {
    if (options.resourceTypes != null && options.resourceTypes.length > 0) {
      throw new KafkaNonRetriableError(
        'ListConfigResources v0 does not support resourceTypes; this broker needs ListConfigResources v1 or newer',
      );
    }
    return {
      request: listConfigResourcesRequestV0({}),
      response: listConfigResourcesResponseV0,
    };
  },
  1: (options) => ({
    request: listConfigResourcesRequestV1({ resourceTypes: options.resourceTypes ?? [] }),
    response: listConfigResourcesResponseV1,
  }),
} satisfies VersionFactories<ListConfigResourcesOptions>;

/**
 * ListConfigResources (key 74). v0 lists client metrics names only
 * (ListClientMetricsResources). v1 adds resourceTypes (KIP-1142).
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export const ListConfigResources = defineRequestFamily(KAFKA_APIS.ListConfigResources, VERSIONS);
