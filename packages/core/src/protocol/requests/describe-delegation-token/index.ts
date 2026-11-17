import { defineRequestFamily, type VersionFactories } from '../index';
import { KAFKA_APIS } from '../kafka-apis';
import { describeDelegationTokenRequestV0 } from './v0/request';
import { describeDelegationTokenResponseV0 } from './v0/response';
import { describeDelegationTokenRequestV1 } from './v1/request';
import { describeDelegationTokenResponseV1 } from './v1/response';
import { describeDelegationTokenRequestV2 } from './v2/request';
import { describeDelegationTokenResponseV2 } from './v2/response';
import { describeDelegationTokenRequestV3 } from './v3/request';
import { describeDelegationTokenResponseV3 } from './v3/response';

export interface DescribeDelegationTokenOptions {
  owners?: { principalType: string; name: string }[] | null;
}

function compactOwners(options: DescribeDelegationTokenOptions) {
  return options.owners == null || options.owners.length === 0 ? null : options.owners;
}

const VERSIONS = {
  0: (options) => ({
    request: describeDelegationTokenRequestV0({ owners: options.owners ?? [] }),
    response: describeDelegationTokenResponseV0,
  }),
  1: (options) => ({
    request: describeDelegationTokenRequestV1({ owners: options.owners ?? [] }),
    response: describeDelegationTokenResponseV1,
  }),
  2: (options) => ({
    request: describeDelegationTokenRequestV2({ owners: compactOwners(options) }),
    response: describeDelegationTokenResponseV2,
  }),
  3: (options) => ({
    request: describeDelegationTokenRequestV3({ owners: compactOwners(options) }),
    response: describeDelegationTokenResponseV3,
  }),
} satisfies VersionFactories<DescribeDelegationTokenOptions>;

export const DescribeDelegationToken = defineRequestFamily(KAFKA_APIS.DescribeDelegationToken, VERSIONS);
