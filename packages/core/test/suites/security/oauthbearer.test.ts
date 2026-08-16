import { expect, it } from 'vitest';
import { createAdmin } from '../../../src/admin/index';
import {
  createCluster,
  describeIfOauthbearerEnabled,
  newLogger,
  saslBrokers,
  saslOAuthBearerConnectionOpts,
} from '../../helpers/index';

describeIfOauthbearerEnabled('security.oauthbearer', () => {
  it('connects with an unsecured OAUTHBEARER token', async () => {
    const admin = createAdmin({
      cluster: createCluster(saslOAuthBearerConnectionOpts(), saslBrokers()),
      logger: newLogger(),
    });
    try {
      await admin.connect();
      const cluster = await admin.describeCluster();
      expect(cluster.brokers.length).toBe(3);
    } finally {
      await admin.disconnect();
    }
  });
});
