import { KafkaNonRetriableError } from '../errors';
import { retrier } from '../retry/index';
import type { AdminContext } from './helpers';
import { formatUnknown, protocolType } from './helpers';
import type {
  CreateDelegationTokenOptions,
  CreateDelegationTokenResult,
  DelegationToken,
  DescribeDelegationTokenOptions,
  ExpireDelegationTokenOptions,
  KafkaPrincipal,
  RenewDelegationTokenOptions,
} from './types';

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

export function createDelegationTokensApi({ cluster, logger, retry }: AdminContext): DelegationTokensApi {
  const createDelegationToken = async (
    options: CreateDelegationTokenOptions = {},
  ): Promise<CreateDelegationTokenResult> => {
    const { renewers, maxLifeTimeMs, owner } = options;
    assertPrincipals('renewer', renewers);
    assertOptionalBigint('maxLifeTimeMs', maxLifeTimeMs);
    if (owner !== undefined && !isKafkaPrincipal(owner)) {
      throw new KafkaNonRetriableError(`Invalid owner principal ${formatUnknown(owner)}`);
    }

    return retrier(retry)(async (bail, retryCount, retryTime) => {
      try {
        await cluster.refreshMetadata();
        const broker = await cluster.findControllerBroker();
        const response = await broker.createDelegationToken({
          renewers,
          maxLifetimeMs: maxLifeTimeMs,
          owner,
        });
        return {
          owner: toOwner(response.principalType, response.principalName),
          tokenRequester: toRequester(response.tokenRequesterPrincipalType, response.tokenRequesterPrincipalName),
          issueTimestamp: response.issueTimestampMs,
          expiryTimestamp: response.expiryTimestampMs,
          maxTimestamp: response.maxTimestampMs,
          tokenId: response.tokenId,
          hmac: response.hmac,
        };
      } catch (error) {
        if (protocolType(error) === 'NOT_CONTROLLER') {
          logger.warn('Could not create a delegation token', {
            error: error instanceof Error ? error.message : String(error),
            retryCount,
            retryTime,
          });
          await cluster.refreshMetadata();
          throw error;
        }
        bail(error as Error);
        throw error;
      }
    });
  };

  const renewDelegationToken = async ({
    hmac,
    renewTimePeriodMs,
  }: RenewDelegationTokenOptions): Promise<{ expiryTimestamp: bigint }> => {
    assertHmac(hmac);
    assertOptionalBigint('renewTimePeriodMs', renewTimePeriodMs);

    return retrier(retry)(async (bail, retryCount, retryTime) => {
      try {
        await cluster.refreshMetadata();
        const broker = await cluster.findControllerBroker();
        const { expiryTimestampMs } = await broker.renewDelegationToken({
          hmac,
          renewPeriodMs: renewTimePeriodMs,
        });
        return { expiryTimestamp: expiryTimestampMs };
      } catch (error) {
        if (protocolType(error) === 'NOT_CONTROLLER') {
          logger.warn('Could not renew a delegation token', {
            error: error instanceof Error ? error.message : String(error),
            retryCount,
            retryTime,
          });
          await cluster.refreshMetadata();
          throw error;
        }
        bail(error as Error);
        throw error;
      }
    });
  };

  const expireDelegationToken = async ({
    hmac,
    expiryTimePeriodMs,
  }: ExpireDelegationTokenOptions): Promise<{ expiryTimestamp: bigint }> => {
    assertHmac(hmac);
    assertOptionalBigint('expiryTimePeriodMs', expiryTimePeriodMs);

    return retrier(retry)(async (bail, retryCount, retryTime) => {
      try {
        await cluster.refreshMetadata();
        const broker = await cluster.findControllerBroker();
        const { expiryTimestampMs } = await broker.expireDelegationToken({ hmac, expiryTimePeriodMs });
        return { expiryTimestamp: expiryTimestampMs };
      } catch (error) {
        if (protocolType(error) === 'NOT_CONTROLLER') {
          logger.warn('Could not expire a delegation token', {
            error: error instanceof Error ? error.message : String(error),
            retryCount,
            retryTime,
          });
          await cluster.refreshMetadata();
          throw error;
        }
        bail(error as Error);
        throw error;
      }
    });
  };

  const describeDelegationToken = async (
    options: DescribeDelegationTokenOptions = {},
  ): Promise<{ tokens: DelegationToken[] }> => {
    assertPrincipals('owner', options.owners);

    return retrier(retry)(async (bail, retryCount, retryTime) => {
      try {
        await cluster.refreshMetadata();
        const broker = await cluster.findControllerBroker();
        const { tokens } = await broker.describeDelegationToken({ owners: options.owners });
        return {
          tokens: tokens.map((token) => ({
            owner: toOwner(token.principalType, token.principalName),
            tokenRequester: toRequester(token.tokenRequesterPrincipalType, token.tokenRequesterPrincipalName),
            issueTimestamp: token.issueTimestamp,
            expiryTimestamp: token.expiryTimestamp,
            maxTimestamp: token.maxTimestamp,
            tokenId: token.tokenId,
            hmac: token.hmac,
            renewers: token.renewers,
          })),
        };
      } catch (error) {
        if (protocolType(error) === 'NOT_CONTROLLER') {
          logger.warn('Could not describe delegation tokens', {
            error: error instanceof Error ? error.message : String(error),
            retryCount,
            retryTime,
          });
          await cluster.refreshMetadata();
          throw error;
        }
        bail(error as Error);
        throw error;
      }
    });
  };

  return { createDelegationToken, renewDelegationToken, expireDelegationToken, describeDelegationToken };
}
