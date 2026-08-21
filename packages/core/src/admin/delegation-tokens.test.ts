import { randomBytes, randomUUID } from 'node:crypto';
import { describe, expect, it, vi } from 'vitest';
import type { Cluster } from '../cluster/index';
import { KafkaNonRetriableError } from '../errors';
import { createLogger, LOG_LEVELS } from '../loggers/index';
import { createDelegationTokensApi } from './delegation-tokens';

const silentLogger = createLogger({ level: LOG_LEVELS.NOTHING, logCreator: () => () => {} });

const hmac = randomBytes(16);
const tokenId = `token-${randomUUID()}`;
const ownerName = `user-${randomUUID()}`;
const requesterName = `user-${randomUUID()}`;
const renewerName = `user-${randomUUID()}`;
const created = {
  principalType: 'User',
  principalName: ownerName,
  tokenRequesterPrincipalType: 'User',
  tokenRequesterPrincipalName: requesterName,
  issueTimestampMs: 1_700_000_000_000n,
  expiryTimestampMs: 1_700_003_600_000n,
  maxTimestampMs: 1_700_007_200_000n,
  tokenId,
  hmac,
};

function fakeCluster(
  methods: {
    createDelegationToken?: ReturnType<typeof vi.fn>;
    renewDelegationToken?: ReturnType<typeof vi.fn>;
    expireDelegationToken?: ReturnType<typeof vi.fn>;
    describeDelegationToken?: ReturnType<typeof vi.fn>;
  } = {},
) {
  const broker = {
    createDelegationToken: methods.createDelegationToken ?? vi.fn().mockResolvedValue(created),
    renewDelegationToken: methods.renewDelegationToken ?? vi.fn().mockResolvedValue({ expiryTimestampMs: 1n }),
    expireDelegationToken: methods.expireDelegationToken ?? vi.fn().mockResolvedValue({ expiryTimestampMs: 2n }),
    describeDelegationToken: methods.describeDelegationToken ?? vi.fn().mockResolvedValue({ tokens: [] }),
  };
  return {
    refreshMetadata: vi.fn().mockResolvedValue(undefined),
    findControllerBroker: vi.fn().mockResolvedValue(broker),
    broker,
  };
}

function apiFor(cluster: ReturnType<typeof fakeCluster>) {
  return createDelegationTokensApi({
    cluster: cluster as unknown as Cluster,
    logger: silentLogger.namespace('Admin'),
    rootLogger: silentLogger,
  });
}

describe('admin/delegation-tokens', () => {
  it('creates a token on the controller and maps principals and bigint timestamps', async () => {
    const cluster = fakeCluster();
    const api = apiFor(cluster);
    const renewers = [{ principalType: 'User', name: renewerName }];

    await expect(
      api.createDelegationToken({
        renewers,
        maxLifeTimeMs: 3_600_000n,
        owner: { principalType: 'User', name: ownerName },
      }),
    ).resolves.toEqual({
      owner: { principalType: 'User', name: ownerName },
      tokenRequester: { principalType: 'User', name: requesterName },
      issueTimestamp: 1_700_000_000_000n,
      expiryTimestamp: 1_700_003_600_000n,
      maxTimestamp: 1_700_007_200_000n,
      tokenId,
      hmac,
    });

    expect(cluster.refreshMetadata).toHaveBeenCalled();
    expect(cluster.findControllerBroker).toHaveBeenCalled();
    expect(cluster.broker.createDelegationToken).toHaveBeenCalledWith({
      renewers,
      maxLifetimeMs: 3_600_000n,
      owner: { principalType: 'User', name: ownerName },
    });
  });

  it('renews and expires a token by hmac', async () => {
    const cluster = fakeCluster();
    const api = apiFor(cluster);

    await expect(api.renewDelegationToken({ hmac, renewTimePeriodMs: 60_000n })).resolves.toEqual({
      expiryTimestamp: 1n,
    });
    await expect(api.expireDelegationToken({ hmac })).resolves.toEqual({ expiryTimestamp: 2n });

    expect(cluster.broker.renewDelegationToken).toHaveBeenCalledWith({ hmac, renewPeriodMs: 60_000n });
    expect(cluster.broker.expireDelegationToken).toHaveBeenCalledWith({ hmac, expiryTimePeriodMs: undefined });
  });

  it('describes tokens and maps renewers', async () => {
    const cluster = fakeCluster({
      describeDelegationToken: vi.fn().mockResolvedValue({
        tokens: [
          {
            principalType: 'User',
            principalName: ownerName,
            tokenRequesterPrincipalType: 'User',
            tokenRequesterPrincipalName: requesterName,
            issueTimestamp: 1n,
            expiryTimestamp: 2n,
            maxTimestamp: 3n,
            tokenId,
            hmac,
            renewers: [{ principalType: 'User', name: renewerName }],
          },
        ],
      }),
    });
    const api = apiFor(cluster);

    await expect(
      api.describeDelegationToken({ owners: [{ principalType: 'User', name: ownerName }] }),
    ).resolves.toEqual({
      tokens: [
        {
          owner: { principalType: 'User', name: ownerName },
          tokenRequester: { principalType: 'User', name: requesterName },
          issueTimestamp: 1n,
          expiryTimestamp: 2n,
          maxTimestamp: 3n,
          tokenId,
          hmac,
          renewers: [{ principalType: 'User', name: renewerName }],
        },
      ],
    });
  });

  it('rejects invalid principals and hmac before contacting the controller', async () => {
    const cluster = fakeCluster();
    const api = apiFor(cluster);

    await expect(api.createDelegationToken({ renewers: [{ principalType: '', name: ownerName }] })).rejects.toThrow(
      KafkaNonRetriableError,
    );
    await expect(api.renewDelegationToken({ hmac: Buffer.alloc(0) })).rejects.toThrow(KafkaNonRetriableError);
    await expect(api.expireDelegationToken({ hmac: 'not-a-buffer' as unknown as Buffer })).rejects.toThrow(
      KafkaNonRetriableError,
    );
    expect(cluster.findControllerBroker).not.toHaveBeenCalled();
  });
});
