import { defineRequestFamily, type VersionFactories } from '../index';
import { KAFKA_APIS } from '../kafka-apis';
import { renewDelegationTokenRequestV0 } from './v0/request';
import { renewDelegationTokenResponseV0 } from './v0/response';
import { renewDelegationTokenRequestV1 } from './v1/request';
import { renewDelegationTokenResponseV1 } from './v1/response';
import { renewDelegationTokenRequestV2 } from './v2/request';
import { renewDelegationTokenResponseV2 } from './v2/response';

export interface RenewDelegationTokenOptions {
  hmac: Buffer;
  renewPeriodMs?: bigint;
}

const DEFAULT_RENEW_PERIOD_MS = -1n;

function fields(options: RenewDelegationTokenOptions) {
  return { hmac: options.hmac, renewPeriodMs: options.renewPeriodMs ?? DEFAULT_RENEW_PERIOD_MS };
}

const VERSIONS = {
  0: (options) => ({
    request: renewDelegationTokenRequestV0(fields(options)),
    response: renewDelegationTokenResponseV0,
  }),
  1: (options) => ({
    request: renewDelegationTokenRequestV1(fields(options)),
    response: renewDelegationTokenResponseV1,
  }),
  2: (options) => ({
    request: renewDelegationTokenRequestV2(fields(options)),
    response: renewDelegationTokenResponseV2,
  }),
} satisfies VersionFactories<RenewDelegationTokenOptions>;

export const RenewDelegationToken = defineRequestFamily(KAFKA_APIS.RenewDelegationToken, VERSIONS);
