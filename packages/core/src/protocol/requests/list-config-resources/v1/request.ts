import { compactArray, defineRequest, field, flexibleObject, int8 } from '../../../schema';
import { API_KEYS } from '../../api-keys';

export interface ListConfigResourcesRequestV1Fields {
  resourceTypes: number[];
}

/**
 * ListConfigResources Request (Version: 1) => [resource_types] TAG_BUFFER
 *   resource_types => INT8
 *
 * An empty resourceTypes list asks the broker for its default supported types (KIP-1142).
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export const requestSchema = flexibleObject([field('resourceTypes', compactArray(int8))]);

export const listConfigResourcesRequestV1 = defineRequest({
  apiKey: API_KEYS.ListConfigResources,
  apiVersion: 1,
  apiName: 'ListConfigResources',
  schema: requestSchema,
});
