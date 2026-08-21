import { compactArray, compactString, defineRequest, field, flexibleObject, int64 } from '../../../schema';
import { API_KEYS } from '../../api-keys';

export interface ListTransactionsRequestV0Fields {
  stateFilters: string[];
  producerIdFilters: bigint[];
}

/**
 * ListTransactions Request (Version: 0) => [state_filters] [producer_id_filters] TAG_BUFFER
 *   state_filters => COMPACT_STRING
 *   producer_id_filters => INT64
 *
 * Flexible from v0. Request header v2's trailing TAG_BUFFER is written by `createRequest`.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export const requestSchema = flexibleObject([
  field('stateFilters', compactArray(compactString)),
  field('producerIdFilters', compactArray(int64)),
]);

export const listTransactionsRequestV0 = defineRequest({
  apiKey: API_KEYS.ListTransactions,
  apiVersion: 0,
  apiName: 'ListTransactions',
  schema: requestSchema,
});
