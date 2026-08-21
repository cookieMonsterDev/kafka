import { defineRequest, field, nullableArray, object, string } from '../../../schema';
import { API_KEYS } from '../../api-keys';

export interface DescribeDelegationTokenOwner {
  principalType: string;
  name: string;
}

export interface DescribeDelegationTokenRequestV0Fields {
  owners: DescribeDelegationTokenOwner[];
}

/**
 * DescribeDelegationToken Request (Version: 0) => [owners]
 *   owners => principal_type principal_name
 *
 * `owners` is nullable: empty/null describes every token. Version 1 is the same as version 0.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const ownerSchema = object([field('principalType', string), field('name', string)]);
export const requestSchema = object([field('owners', nullableArray(ownerSchema))]);

export const describeDelegationTokenRequestV0 = defineRequest({
  apiKey: API_KEYS.DescribeDelegationToken,
  apiVersion: 0,
  apiName: 'DescribeDelegationToken',
  schema: requestSchema,
});
