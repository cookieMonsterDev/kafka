import { boolean, compactArray, compactString, defineRequest, field, flexibleObject } from '../../../schema';
import { API_KEYS } from '../../api-keys';

export interface StreamsGroupDescribeRequestV0Fields {
  groupIds: string[];
  includeAuthorizedOperations: boolean;
}

/**
 * StreamsGroupDescribe Request (Version: 0) => [group_ids] include_authorized_operations TAG_BUFFER
 *   group_ids => COMPACT_STRING
 *   include_authorized_operations => BOOLEAN
 *
 * Flexible from v0 (KIP-1071). Request header v2's trailing TAG_BUFFER is written by `createRequest`.
 *
 * @see https://kafka.apache.org/44/design/protocol/
 */
export const requestSchema = flexibleObject([
  field('groupIds', compactArray(compactString)),
  field('includeAuthorizedOperations', boolean),
]);

export const streamsGroupDescribeRequestV0 = defineRequest({
  apiKey: API_KEYS.StreamsGroupDescribe,
  apiVersion: 0,
  apiName: 'StreamsGroupDescribe',
  schema: requestSchema,
});
