import { compactArray, compactString, defineRequest, field, flexibleObject, int64 } from '../../../schema';
import { API_KEYS } from '../../api-keys';
import type { ListTransactionsRequestV0Fields } from '../v0/request';

export interface ListTransactionsRequestV1Fields extends ListTransactionsRequestV0Fields {
  durationFilter: bigint;
}

/**
 * ListTransactions Request (Version: 1) => [state_filters] [producer_id_filters] duration_filter TAG_BUFFER
 *
 * Adds `durationFilter` (KIP-994). Values below 0 mean no duration filter.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export const requestSchema = flexibleObject([
  field('stateFilters', compactArray(compactString)),
  field('producerIdFilters', compactArray(int64)),
  field('durationFilter', int64),
]);

export const listTransactionsRequestV1 = defineRequest({
  apiKey: API_KEYS.ListTransactions,
  apiVersion: 1,
  apiName: 'ListTransactions',
  schema: requestSchema,
});
