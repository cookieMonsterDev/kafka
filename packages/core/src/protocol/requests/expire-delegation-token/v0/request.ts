import { bytes, defineRequest, field, int64, object } from '../../../schema';
import { API_KEYS } from '../../api-keys';

export interface ExpireDelegationTokenRequestV0Fields {
  hmac: Buffer;
  expiryTimePeriodMs: bigint;
}

/**
 * ExpireDelegationToken Request (Version: 0) => hmac expiry_time_period
 *   hmac => BYTES
 *   expiry_time_period => INT64
 *
 * Version 1 is the same as version 0. `-1` expires the token immediately.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export const requestSchema = object([field('hmac', bytes), field('expiryTimePeriodMs', int64)]);

export const expireDelegationTokenRequestV0 = defineRequest({
  apiKey: API_KEYS.ExpireDelegationToken,
  apiVersion: 0,
  apiName: 'ExpireDelegationToken',
  schema: requestSchema,
});
