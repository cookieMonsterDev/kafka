import { afterEach, describe, expect } from 'vitest';
import { createAdmin } from '../../../src/admin/index';
import { KafkaServerDoesNotSupportApiKey } from '../../../src/errors';
import { createCluster, newLogger, testIfKafkaAtLeast_1_1 } from '../../helpers/index';

const TOKEN_UNAVAILABLE = new Set([
  'DELEGATION_TOKEN_AUTH_DISABLED',
  'DELEGATION_TOKEN_REQUEST_NOT_ALLOWED',
  'CLUSTER_AUTHORIZATION_FAILED',
  'SECURITY_DISABLED',
  'ILLEGAL_SASL_STATE',
]);

/**
 * Delegation tokens need `delegation.token.secret.key` and a SASL (not PLAINTEXT) listener.
 * Default compose stacks do not enable that, so create/describe/renew/expire is skipped when
 * the broker returns a "tokens disabled" error. SASL login *with* a token is feature 4.2.
 */
describe('admin.delegation-tokens', () => {
  let admin: ReturnType<typeof createAdmin> | undefined;
  let hmac: Buffer | undefined;

  afterEach(async () => {
    if (admin && hmac) {
      await admin.expireDelegationToken({ hmac }).catch(() => undefined);
    }
    hmac = undefined;
    await admin?.disconnect();
  });

  testIfKafkaAtLeast_1_1('creates, describes, renews, and expires a token when the broker enables them', async () => {
    admin = createAdmin({ cluster: createCluster(), logger: newLogger() });
    await admin.connect();

    let created: Awaited<ReturnType<typeof admin.createDelegationToken>>;
    try {
      created = await admin.createDelegationToken({
        renewers: [{ principalType: 'User', name: 'test' }],
        maxLifeTimeMs: 3_600_000n,
      });
    } catch (error) {
      if (error instanceof KafkaServerDoesNotSupportApiKey) return;
      expect(TOKEN_UNAVAILABLE.has((error as { type?: string }).type ?? '')).toBe(true);
      return;
    }

    hmac = created.hmac;
    expect(created.tokenId.length).toBeGreaterThan(0);
    expect(created.hmac.length).toBeGreaterThan(0);
    expect(created.owner.principalType).toBe('User');
    expect(typeof created.issueTimestamp).toBe('bigint');
    expect(typeof created.expiryTimestamp).toBe('bigint');
    expect(typeof created.maxTimestamp).toBe('bigint');

    const described = await admin.describeDelegationToken();
    expect(described.tokens.some((token) => token.tokenId === created.tokenId)).toBe(true);

    const renewed = await admin.renewDelegationToken({ hmac: created.hmac, renewTimePeriodMs: 1_800_000n });
    expect(typeof renewed.expiryTimestamp).toBe('bigint');

    const expired = await admin.expireDelegationToken({ hmac: created.hmac, expiryTimePeriodMs: -1n });
    expect(typeof expired.expiryTimestamp).toBe('bigint');
    hmac = undefined;
  });
});
