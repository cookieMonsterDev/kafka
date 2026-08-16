import { afterEach, describe, expect, it } from 'vitest';
import { createAdmin } from '../../src/admin/index.js';
import {
  createCluster,
  newLogger,
  saslBrokers,
  saslEntries,
  sslBrokers,
  sslConnectionOpts,
} from '../helpers/index.js';

describe('admin.connection', () => {
  let admin: ReturnType<typeof createAdmin> | undefined;

  afterEach(async () => {
    await admin?.disconnect();
  });

  it('connects over PLAINTEXT', async () => {
    admin = createAdmin({ cluster: createCluster(), logger: newLogger() });
    await admin.connect();
  });

  it('connects over SSL', async () => {
    admin = createAdmin({ cluster: createCluster(sslConnectionOpts(), sslBrokers()), logger: newLogger() });
    await admin.connect();
  });

  it.each(saslEntries)('connects over SASL $name', async (entry) => {
    admin = createAdmin({ cluster: createCluster(entry.opts(), saslBrokers()), logger: newLogger() });
    await admin.connect();
  });
});
