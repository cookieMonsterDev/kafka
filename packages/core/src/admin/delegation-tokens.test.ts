import { describe, expect, it, vi } from 'vitest';
import type { Cluster } from '../cluster/index';
import { KafkaNonRetriableError } from '../errors';
import { createLogger, LOG_LEVELS } from '../loggers/index';
import { createDelegationTokensApi } from './delegation-tokens';

const silentLogger = createLogger({ level: LOG_LEVELS.NOTHING, logCreator: () => () => {} });

const hmac = Buffer.from([1, 2, 3, 4]);
const created = {
  principalType: 'User',
  principalName: 'alice',
  tokenRequesterPrincipalType: 'User',
  tokenRequesterPrincipalName: 'admin',
  issueTimestampMs: 1_700_000_000_000n,
  expiryTimestampMs: 1_700_003_600_000n,
  maxTimestampMs: 1_700_007_200_000n,
  tokenId: 'token-id',
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
    const renewers = [{ principalType: 'User', name: 'bob' }];

    await expect(
      api.createDelegationToken({
        renewers,
        maxLifeTimeMs: 3_600_000n,
        owner: { principalType: 'User', name: 'alice' },
      }),
    ).resolves.toEqual({
      owner: { principalType: 'User', name: 'alice' },
      tokenRequester: { principalType: 'User', name: 'admin' },
      issueTimestamp: 1_700_000_000_000n,
      expiryTimestamp: 1_700_003_600_000n,
      maxTimestamp: 1_700_007_200_000n,
      tokenId: 'token-id',
      hmac,
    });

    expect(cluster.refreshMetadata).toHaveBeenCalled();
    expect(cluster.findControllerBroker).toHaveBeenCalled();
    expect(cluster.broker.createDelegationToken).toHaveBeenCalledWith({
      renewers,
      maxLifetimeMs: 3_600_000n,
      owner: { principalType: 'User', name: 'alice' },
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
            principalName: 'alice',
            tokenRequesterPrincipalType: 'User',
            tokenRequesterPrincipalName: 'admin',
            issueTimestamp: 1n,
            expiryTimestamp: 2n,
            maxTimestamp: 3n,
            tokenId: 'token-id',
            hmac,
            renewers: [{ principalType: 'User', name: 'bob' }],
          },
        ],
      }),
    });
    const api = apiFor(cluster);

    await expect(api.describeDelegationToken({ owners: [{ principalType: 'User', name: 'alice' }] })).resolves.toEqual({
      tokens: [
        {
          owner: { principalType: 'User', name: 'alice' },
          tokenRequester: { principalType: 'User', name: 'admin' },
          issueTimestamp: 1n,
          expiryTimestamp: 2n,
          maxTimestamp: 3n,
          tokenId: 'token-id',
          hmac,
          renewers: [{ principalType: 'User', name: 'bob' }],
        },
      ],
    });
  });

  it('rejects invalid principals and hmac before contacting the controller', async () => {
    const cluster = fakeCluster();
    const api = apiFor(cluster);

    await expect(api.createDelegationToken({ renewers: [{ principalType: '', name: 'alice' }] })).rejects.toThrow(
      KafkaNonRetriableError,
    );
    await expect(api.renewDelegationToken({ hmac: Buffer.alloc(0) })).rejects.toThrow(KafkaNonRetriableError);
    await expect(api.expireDelegationToken({ hmac: 'not-a-buffer' as unknown as Buffer })).rejects.toThrow(
      KafkaNonRetriableError,
    );
    expect(cluster.findControllerBroker).not.toHaveBeenCalled();
  });
});
