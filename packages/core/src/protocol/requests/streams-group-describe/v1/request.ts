import { boolean, compactArray, compactString, defineRequest, field, flexibleObject } from '../../../schema';
import { API_KEYS } from '../../api-keys';
import type { StreamsGroupDescribeRequestV0Fields } from '../v0/request';

export interface StreamsGroupDescribeRequestV1Fields extends StreamsGroupDescribeRequestV0Fields {
  includeTopologyDescription: boolean;
}

/**
 * StreamsGroupDescribe Request (Version: 1) => [group_ids] include_authorized_operations include_topology_description TAG_BUFFER
 *   group_ids => COMPACT_STRING
 *   include_authorized_operations => BOOLEAN
 *   include_topology_description => BOOLEAN
 *
 * Version 1 adds `include_topology_description` (KIP-1331).
 *
 * @see https://kafka.apache.org/44/design/protocol/
 */
export const requestSchema = flexibleObject([
  field('groupIds', compactArray(compactString)),
  field('includeAuthorizedOperations', boolean),
  field('includeTopologyDescription', boolean),
]);

export const streamsGroupDescribeRequestV1 = defineRequest({
  apiKey: API_KEYS.StreamsGroupDescribe,
  apiVersion: 1,
  apiName: 'StreamsGroupDescribe',
  schema: requestSchema,
});
