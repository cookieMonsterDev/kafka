import { afterEach, describe, expect } from 'vitest';
import { createAdmin } from '../../../src/admin/index';
import { createCluster, newLogger, testIfKafkaAtLeast_1_1 } from '../../helpers/index';

describe('admin.logDirs', () => {
  let admin: ReturnType<typeof createAdmin> | undefined;

  afterEach(async () => {
    await admin?.disconnect();
  });

  testIfKafkaAtLeast_1_1('describes log dirs on every broker', async () => {
    admin = createAdmin({ cluster: createCluster(), logger: newLogger() });
    await admin.connect();

    const described = await admin.describeLogDirs();
    expect(described.brokers.length).toBeGreaterThan(0);
    expect(described.brokers[0]?.logDirs.length).toBeGreaterThan(0);
    expect(described.brokers[0]?.logDirs[0]).toEqual(
      expect.objectContaining({
        logDir: expect.any(String),
        errorCode: 0,
      }),
    );
  });
});
