import { boolean, compactArray, compactString, defineRequest, field, flexibleObject } from '../../../schema';
import { API_KEYS } from '../../api-keys';

export interface ShareGroupDescribeRequestV1Fields {
  groupIds: string[];
  includeAuthorizedOperations: boolean;
}

/**
 * ShareGroupDescribe Request (Version: 1) => [group_ids] include_authorized_operations TAG_BUFFER
 *   group_ids => COMPACT_STRING
 *   include_authorized_operations => BOOLEAN
 *
 * Flexible from v0 (KIP-932). Same shape as ConsumerGroupDescribe v0. Request header v2's trailing
 * TAG_BUFFER is written by `createRequest`.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export const requestSchema = flexibleObject([
  field('groupIds', compactArray(compactString)),
  field('includeAuthorizedOperations', boolean),
]);

export const shareGroupDescribeRequestV1 = defineRequest({
  apiKey: API_KEYS.ShareGroupDescribe,
  apiVersion: 1,
  apiName: 'ShareGroupDescribe',
  schema: requestSchema,
});
