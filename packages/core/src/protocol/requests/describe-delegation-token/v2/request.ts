import { compactNullableArray, compactString, defineRequest, field, flexibleObject } from '../../../schema';
import { API_KEYS } from '../../api-keys';
import type { DescribeDelegationTokenOwner } from '../v0/request';

export interface DescribeDelegationTokenRequestV2Fields {
  owners: DescribeDelegationTokenOwner[] | null;
}

/**
 * DescribeDelegationToken Request (Version: 2) => [owners] TAG_BUFFER
 *   owners => principal_type principal_name TAG_BUFFER
 *
 * Flexible compact + tagged form of v1. `owners: null` describes every token.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const ownerSchema = flexibleObject([field('principalType', compactString), field('name', compactString)]);
export const requestSchema = flexibleObject([field('owners', compactNullableArray(ownerSchema))]);

export const describeDelegationTokenRequestV2 = defineRequest({
  apiKey: API_KEYS.DescribeDelegationToken,
  apiVersion: 2,
  apiName: 'DescribeDelegationToken',
  schema: requestSchema,
});
