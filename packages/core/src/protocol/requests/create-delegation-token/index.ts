import { KafkaNonRetriableError } from '../../../errors';
import { defineRequestFamily, type VersionFactories } from '../index';
import { KAFKA_APIS } from '../kafka-apis';
import { createDelegationTokenRequestV0 } from './v0/request';
import { createDelegationTokenResponseV0 } from './v0/response';
import { createDelegationTokenRequestV1 } from './v1/request';
import { createDelegationTokenResponseV1 } from './v1/response';
import { createDelegationTokenRequestV2 } from './v2/request';
import { createDelegationTokenResponseV2 } from './v2/response';
import { createDelegationTokenRequestV3 } from './v3/request';
import { createDelegationTokenResponseV3 } from './v3/response';

export interface CreateDelegationTokenOptions {
  renewers?: { principalType: string; name: string }[];
  maxLifetimeMs?: bigint;
  owner?: { principalType: string; name: string } | null;
}

const DEFAULT_MAX_LIFETIME_MS = -1n;

function assertNoOwner(options: CreateDelegationTokenOptions): void {
  if (options.owner != null) {
    throw new KafkaNonRetriableError(
      'CreateDelegationToken owner principal requires version 3 (Kafka 3.3+); this broker negotiated an older version',
    );
  }
}

function v0to2Fields(options: CreateDelegationTokenOptions) {
  return {
    renewers: options.renewers ?? [],
    maxLifetimeMs: options.maxLifetimeMs ?? DEFAULT_MAX_LIFETIME_MS,
  };
}

const VERSIONS = {
  0: (options) => {
    assertNoOwner(options);
    return { request: createDelegationTokenRequestV0(v0to2Fields(options)), response: createDelegationTokenResponseV0 };
  },
  1: (options) => {
    assertNoOwner(options);
    return { request: createDelegationTokenRequestV1(v0to2Fields(options)), response: createDelegationTokenResponseV1 };
  },
  2: (options) => {
    assertNoOwner(options);
    return { request: createDelegationTokenRequestV2(v0to2Fields(options)), response: createDelegationTokenResponseV2 };
  },
  3: (options) => ({
    request: createDelegationTokenRequestV3({
      ownerPrincipalType: options.owner?.principalType ?? null,
      ownerPrincipalName: options.owner?.name ?? null,
      ...v0to2Fields(options),
    }),
    response: createDelegationTokenResponseV3,
  }),
} satisfies VersionFactories<CreateDelegationTokenOptions>;

export const CreateDelegationToken = defineRequestFamily(KAFKA_APIS.CreateDelegationToken, VERSIONS);
