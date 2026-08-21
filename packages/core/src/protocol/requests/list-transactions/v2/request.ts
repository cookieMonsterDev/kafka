import {
  compactArray,
  compactNullableString,
  compactString,
  defineRequest,
  field,
  flexibleObject,
  int64,
} from '../../../schema';
import { API_KEYS } from '../../api-keys';
import type { ListTransactionsRequestV1Fields } from '../v1/request';

export interface ListTransactionsRequestV2Fields extends ListTransactionsRequestV1Fields {
  transactionalIdPattern: string | null;
}

/**
 * ListTransactions Request (Version: 2) => [state_filters] [producer_id_filters] duration_filter
 *                                          transactional_id_pattern TAG_BUFFER
 *
 * Adds `transactionalIdPattern` (KIP-1152). Null or empty means no pattern filter.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export const requestSchema = flexibleObject([
  field('stateFilters', compactArray(compactString)),
  field('producerIdFilters', compactArray(int64)),
  field('durationFilter', int64),
  field('transactionalIdPattern', compactNullableString),
]);

export const listTransactionsRequestV2 = defineRequest({
  apiKey: API_KEYS.ListTransactions,
  apiVersion: 2,
  apiName: 'ListTransactions',
  schema: requestSchema,
});
