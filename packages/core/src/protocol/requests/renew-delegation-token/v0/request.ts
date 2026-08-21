import { bytes, defineRequest, field, int64, object } from '../../../schema';
import { API_KEYS } from '../../api-keys';

export interface RenewDelegationTokenRequestV0Fields {
  hmac: Buffer;
  renewPeriodMs: bigint;
}

/**
 * RenewDelegationToken Request (Version: 0) => hmac renew_period
 *   hmac => BYTES
 *   renew_period => INT64
 *
 * Version 1 is the same as version 0.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export const requestSchema = object([field('hmac', bytes), field('renewPeriodMs', int64)]);

export const renewDelegationTokenRequestV0 = defineRequest({
  apiKey: API_KEYS.RenewDelegationToken,
  apiVersion: 0,
  apiName: 'RenewDelegationToken',
  schema: requestSchema,
});
