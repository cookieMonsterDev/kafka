import { KafkaNonRetriableError } from '../errors';
import { CONTROLLER } from '../cluster/run';
import { retrier as createRetrier } from '../retry/index';
import type { AdminContext } from './helpers';
import { formatUnknown } from './helpers';
import type {
  CreateDelegationTokenOptions,
  CreateDelegationTokenResult,
  DelegationToken,
  DescribeDelegationTokenOptions,
  ExpireDelegationTokenOptions,
  KafkaPrincipal,
  RenewDelegationTokenOptions,
} from './types';
import {
  CreateDelegationToken,
  DescribeDelegationToken,
  ExpireDelegationToken,
  RenewDelegationToken,
} from '../protocol/requests/lazy-families';

export interface DelegationTokensApi {
  createDelegationToken: (options?: CreateDelegationTokenOptions) => Promise<CreateDelegationTokenResult>;
  renewDelegationToken: (options: RenewDelegationTokenOptions) => Promise<{ expiryTimestamp: bigint }>;
  expireDelegationToken: (options: ExpireDelegationTokenOptions) => Promise<{ expiryTimestamp: bigint }>;
  describeDelegationToken: (options?: DescribeDelegationTokenOptions) => Promise<{ tokens: DelegationToken[] }>;
}

function isKafkaPrincipal(value: unknown): value is KafkaPrincipal {
  return (
    value != null &&
    typeof value === 'object' &&
    typeof (value as KafkaPrincipal).principalType === 'string' &&
    (value as KafkaPrincipal).principalType.length > 0 &&
    typeof (value as KafkaPrincipal).name === 'string' &&
    (value as KafkaPrincipal).name.length > 0
  );
}

function assertPrincipals(label: string, values: KafkaPrincipal[] | undefined): void {
  if (values === undefined) return;
  if (!Array.isArray(values)) {
    throw new KafkaNonRetriableError(`Invalid ${label} array ${formatUnknown(values)}`);
  }
  const invalid = values.find((value) => !isKafkaPrincipal(value));
  if (invalid) {
    throw new KafkaNonRetriableError(`Invalid ${label} principal ${formatUnknown(invalid)}`);
  }
}

function assertOptionalBigint(label: string, value: bigint | undefined): void {
  if (value !== undefined && typeof value !== 'bigint') {
    throw new KafkaNonRetriableError(`Invalid ${label} ${formatUnknown(value)}`);
  }
}

function assertHmac(hmac: unknown): asserts hmac is Buffer {
  if (!Buffer.isBuffer(hmac) || hmac.length === 0) {
    throw new KafkaNonRetriableError(`Invalid delegation token hmac ${formatUnknown(hmac)}`);
  }
}

function toOwner(principalType: string, principalName: string): KafkaPrincipal {
  return { principalType, name: principalName };
}

function toRequester(principalType: string | undefined, principalName: string | undefined): KafkaPrincipal | undefined {
  if (principalType == null || principalName == null) return undefined;
  return { principalType, name: principalName };
}

export function createDelegationTokensApi({ cluster, retry }: AdminContext): DelegationTokensApi {
  const retrier = createRetrier(retry);

  const createDelegationToken = async (
    options: CreateDelegationTokenOptions = {},
  ): Promise<CreateDelegationTokenResult> => {
    const { renewers, maxLifeTimeMs, owner } = options;
    assertPrincipals('renewer', renewers);
    assertOptionalBigint('maxLifeTimeMs', maxLifeTimeMs);
    if (owner !== undefined && !isKafkaPrincipal(owner)) {
      throw new KafkaNonRetriableError(`Invalid owner principal ${formatUnknown(owner)}`);
    }

    return cluster.run(
      CONTROLLER,
      async (broker) => {
        const response = await broker.send(CreateDelegationToken, {
          renewers,
          maxLifetimeMs: maxLifeTimeMs,
          owner,
        });
        return {
          owner: toOwner(response.principalType, response.principalName),
          tokenRequester:
            'tokenRequesterPrincipalType' in response
              ? toRequester(response.tokenRequesterPrincipalType, response.tokenRequesterPrincipalName)
              : undefined,
          issueTimestamp: response.issueTimestampMs,
          expiryTimestamp: response.expiryTimestampMs,
          maxTimestamp: response.maxTimestampMs,
          tokenId: response.tokenId,
          hmac: response.hmac,
        };
      },
      { retrier, refreshMetadata: true, description: 'Could not create a delegation token' },
    );
  };

  const renewDelegationToken = async ({
    hmac,
    renewTimePeriodMs,
  }: RenewDelegationTokenOptions): Promise<{ expiryTimestamp: bigint }> => {
    assertHmac(hmac);
    assertOptionalBigint('renewTimePeriodMs', renewTimePeriodMs);

    return cluster.run(
      CONTROLLER,
      async (broker) => {
        const { expiryTimestampMs } = await broker.send(RenewDelegationToken, {
          hmac,
          renewPeriodMs: renewTimePeriodMs,
        });
        return { expiryTimestamp: expiryTimestampMs };
      },
      { retrier, refreshMetadata: true, description: 'Could not renew a delegation token' },
    );
  };

  const expireDelegationToken = async ({
    hmac,
    expiryTimePeriodMs,
  }: ExpireDelegationTokenOptions): Promise<{ expiryTimestamp: bigint }> => {
    assertHmac(hmac);
    assertOptionalBigint('expiryTimePeriodMs', expiryTimePeriodMs);

    return cluster.run(
      CONTROLLER,
      async (broker) => {
        const { expiryTimestampMs } = await broker.send(ExpireDelegationToken, { hmac, expiryTimePeriodMs });
        return { expiryTimestamp: expiryTimestampMs };
      },
      { retrier, refreshMetadata: true, description: 'Could not expire a delegation token' },
    );
  };

  const describeDelegationToken = async (
    options: DescribeDelegationTokenOptions = {},
  ): Promise<{ tokens: DelegationToken[] }> => {
    assertPrincipals('owner', options.owners);

    return cluster.run(
      CONTROLLER,
      async (broker) => {
        const { tokens } = await broker.send(DescribeDelegationToken, { owners: options.owners });
        return {
          tokens: tokens.map((token) => ({
            owner: toOwner(token.principalType, token.principalName),
            tokenRequester:
              'tokenRequesterPrincipalType' in token
                ? toRequester(token.tokenRequesterPrincipalType, token.tokenRequesterPrincipalName)
                : undefined,
            issueTimestamp: token.issueTimestamp,
            expiryTimestamp: token.expiryTimestamp,
            maxTimestamp: token.maxTimestamp,
            tokenId: token.tokenId,
            hmac: token.hmac,
            renewers: token.renewers,
          })),
        };
      },
      { retrier, refreshMetadata: true, description: 'Could not describe delegation tokens' },
    );
  };

  return { createDelegationToken, renewDelegationToken, expireDelegationToken, describeDelegationToken };
}
