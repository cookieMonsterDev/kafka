import { compactArray, compactString, defineRequest, field, flexibleObject } from '../../../schema';
import { API_KEYS } from '../../api-keys';

export interface DescribeTransactionsRequestV0Fields {
  transactionalIds: string[];
}

/**
 * DescribeTransactions Request (Version: 0) => [transactional_ids] TAG_BUFFER
 *   transactional_ids => COMPACT_STRING
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export const requestSchema = flexibleObject([field('transactionalIds', compactArray(compactString))]);

export const describeTransactionsRequestV0 = defineRequest({
  apiKey: API_KEYS.DescribeTransactions,
  apiVersion: 0,
  apiName: 'DescribeTransactions',
  schema: requestSchema,
});
