import {
  boolean,
  compactArray,
  compactNullableString,
  compactString,
  defineRequest,
  field,
  flexibleObject,
  int8,
} from '../../../schema';
import { API_KEYS } from '../../api-keys';

export {
  type DescribeClientQuotasComponent,
  type DescribeClientQuotasRequestV0Fields as DescribeClientQuotasRequestV1Fields,
} from '../v0/request';

/**
 * DescribeClientQuotas Request (Version: 1) => [components] strict TAG_BUFFER
 *   components => entity_type match_type match TAG_BUFFER
 *     entity_type => COMPACT_STRING
 *     match_type => INT8
 *     match => COMPACT_NULLABLE_STRING
 *   strict => BOOLEAN
 *
 * Flexible form of v0 (KIP-482). Request header v2's trailing TAG_BUFFER is written by `createRequest`.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const componentSchema = flexibleObject([
  field('entityType', compactString),
  field('matchType', int8),
  field('match', compactNullableString),
]);
export const requestSchema = flexibleObject([
  field('components', compactArray(componentSchema)),
  field('strict', boolean),
]);

export const describeClientQuotasRequestV1 = defineRequest({
  apiKey: API_KEYS.DescribeClientQuotas,
  apiVersion: 1,
  apiName: 'DescribeClientQuotas',
  schema: requestSchema,
});
