import { compactBytes, defineRequest, field, flexibleObject, int64 } from '../../../schema';
import { API_KEYS } from '../../api-keys';
import type { RenewDelegationTokenRequestV0Fields } from '../v0/request';

export type RenewDelegationTokenRequestV2Fields = RenewDelegationTokenRequestV0Fields;

/**
 * RenewDelegationToken Request (Version: 2) => hmac renew_period_ms TAG_BUFFER
 *
 * Flexible compact + tagged form of v1 (KIP-482).
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export const requestSchema = flexibleObject([field('hmac', compactBytes), field('renewPeriodMs', int64)]);

export const renewDelegationTokenRequestV2 = defineRequest({
  apiKey: API_KEYS.RenewDelegationToken,
  apiVersion: 2,
  apiName: 'RenewDelegationToken',
  schema: requestSchema,
});
