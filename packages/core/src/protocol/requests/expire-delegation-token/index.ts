import { defineRequestFamily, type VersionFactories } from '../index';
import { KAFKA_APIS } from '../kafka-apis';
import { expireDelegationTokenRequestV0 } from './v0/request';
import { expireDelegationTokenResponseV0 } from './v0/response';
import { expireDelegationTokenRequestV1 } from './v1/request';
import { expireDelegationTokenResponseV1 } from './v1/response';
import { expireDelegationTokenRequestV2 } from './v2/request';
import { expireDelegationTokenResponseV2 } from './v2/response';

export interface ExpireDelegationTokenOptions {
  hmac: Buffer;
  expiryTimePeriodMs?: bigint;
}

/** Java Admin uses -1 to expire the token immediately. */
const EXPIRE_IMMEDIATELY_MS = -1n;

function fields(options: ExpireDelegationTokenOptions) {
  return { hmac: options.hmac, expiryTimePeriodMs: options.expiryTimePeriodMs ?? EXPIRE_IMMEDIATELY_MS };
}

const VERSIONS = {
  0: (options) => ({
    request: expireDelegationTokenRequestV0(fields(options)),
    response: expireDelegationTokenResponseV0,
  }),
  1: (options) => ({
    request: expireDelegationTokenRequestV1(fields(options)),
    response: expireDelegationTokenResponseV1,
  }),
  2: (options) => ({
    request: expireDelegationTokenRequestV2(fields(options)),
    response: expireDelegationTokenResponseV2,
  }),
} satisfies VersionFactories<ExpireDelegationTokenOptions>;

export const ExpireDelegationToken = defineRequestFamily(KAFKA_APIS.ExpireDelegationToken, VERSIONS);
