import { boolean, compactArray, compactString, defineRequest, field, flexibleObject } from '../../../schema';
import { API_KEYS } from '../../api-keys';

export interface ConsumerGroupDescribeRequestV0Fields {
  groupIds: string[];
  includeAuthorizedOperations: boolean;
}

/**
 * ConsumerGroupDescribe Request (Version: 0) => [group_ids] include_authorized_operations TAG_BUFFER
 *   group_ids => COMPACT_STRING
 *   include_authorized_operations => BOOLEAN
 *
 * Flexible from v0 (KIP-848). Version 1 of the request is the same as version 0.
 * Request header v2's trailing TAG_BUFFER is written by `createRequest`.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export const requestSchema = flexibleObject([
  field('groupIds', compactArray(compactString)),
  field('includeAuthorizedOperations', boolean),
]);

export const consumerGroupDescribeRequestV0 = defineRequest({
  apiKey: API_KEYS.ConsumerGroupDescribe,
  apiVersion: 0,
  apiName: 'ConsumerGroupDescribe',
  schema: requestSchema,
});
