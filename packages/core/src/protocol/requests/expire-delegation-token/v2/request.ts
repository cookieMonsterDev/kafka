import { compactBytes, defineRequest, field, flexibleObject, int64 } from '../../../schema';
import { API_KEYS } from '../../api-keys';
import type { ExpireDelegationTokenRequestV0Fields } from '../v0/request';

export type ExpireDelegationTokenRequestV2Fields = ExpireDelegationTokenRequestV0Fields;

/**
 * ExpireDelegationToken Request (Version: 2) => hmac expiry_time_period_ms TAG_BUFFER
 *
 * Flexible compact + tagged form of v1 (KIP-482). `-1` expires the token immediately.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export const requestSchema = flexibleObject([field('hmac', compactBytes), field('expiryTimePeriodMs', int64)]);

export const expireDelegationTokenRequestV2 = defineRequest({
  apiKey: API_KEYS.ExpireDelegationToken,
  apiVersion: 2,
  apiName: 'ExpireDelegationToken',
  schema: requestSchema,
});
