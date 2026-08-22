import { defineRequest } from '../../../schema';
import { API_KEYS } from '../../api-keys';
import { requestSchema, type DescribeShareGroupOffsetsRequestV0Fields } from '../v0/request';

export type DescribeShareGroupOffsetsRequestV1Fields = DescribeShareGroupOffsetsRequestV0Fields;

/**
 * DescribeShareGroupOffsets Request (Version: 1) is the same as version 0. Version 1 adds `lag`
 * on the response (KIP-1226).
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export { requestSchema };

export const describeShareGroupOffsetsRequestV1 = defineRequest({
  apiKey: API_KEYS.DescribeShareGroupOffsets,
  apiVersion: 1,
  apiName: 'DescribeShareGroupOffsets',
  schema: requestSchema,
});
