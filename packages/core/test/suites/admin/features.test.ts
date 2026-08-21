import { afterEach, expect } from 'vitest';
import { createAdmin } from '../../../src/admin/index';
import { createCluster, describeIfKRaft, newLogger, secureRandom, testIfKafkaAtLeast_3_6 } from '../../helpers/index';

describeIfKRaft('admin.features', () => {
  let admin: ReturnType<typeof createAdmin> | undefined;

  afterEach(async () => {
    await admin?.disconnect();
  });

  testIfKafkaAtLeast_3_6('validates feature updates through the controller without applying them', async () => {
    admin = createAdmin({ cluster: createCluster(), logger: newLogger() });
    await admin.connect();

    const feature = `unknown.feature.${secureRandom()}`;
    const updating = admin.updateFeatures({
      featureUpdates: [{ feature, maxVersionLevel: 1 }],
      validateOnly: true,
    });

    await expect(updating).rejects.toMatchObject({
      message: expect.stringMatching(/feature|update|unknown/i),
    });
  });
});
