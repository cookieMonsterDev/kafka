import { defineRequest, flexibleObject } from '../../../schema';
import { API_KEYS } from '../../api-keys';

export type ListConfigResourcesRequestV0Fields = Record<string, never>;

/**
 * ListConfigResources Request (Version: 0) => TAG_BUFFER
 *
 * Version 0 is ListClientMetricsResources: empty body besides the flexible tagged-fields
 * buffer. Resource type filtering is v1 (KIP-1142).
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export const requestSchema = flexibleObject([]);

export const listConfigResourcesRequestV0 = defineRequest({
  apiKey: API_KEYS.ListConfigResources,
  apiVersion: 0,
  apiName: 'ListConfigResources',
  schema: requestSchema,
});
