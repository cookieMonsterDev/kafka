import { afterEach, describe, expect } from 'vitest';
import { createAdmin } from '../../../src/admin/index';
import { createCluster, newLogger, secureRandom, testIfKafkaAtLeast_3_0, waitFor } from '../../helpers/index';

describe('admin.quotas', () => {
  let admin: ReturnType<typeof createAdmin> | undefined;
  let clientId: string;

  afterEach(async () => {
    if (admin && clientId) {
      await admin
        .alterClientQuotas({
          entries: [
            {
              entity: [{ entityType: 'client-id', entityName: clientId }],
              ops: [{ key: 'producer_byte_rate', value: 0, remove: true }],
            },
          ],
        })
        .catch(() => undefined);
    }
    await admin?.disconnect();
  });

  testIfKafkaAtLeast_3_0('alters, describes, and removes a client quota', async () => {
    clientId = `quota-client-${secureRandom()}`;
    admin = createAdmin({ cluster: createCluster(), logger: newLogger() });
    await admin.connect();

    await admin.alterClientQuotas({
      entries: [
        {
          entity: [{ entityType: 'client-id', entityName: clientId }],
          ops: [{ key: 'producer_byte_rate', value: 1024, remove: false }],
        },
      ],
    });

    const described = await waitFor(async () => {
      const result = await admin!.describeClientQuotas({
        components: [{ entityType: 'client-id', matchType: 0, match: clientId }],
        strict: true,
      });
      const entry = result.entries.find((item) => item.entity.some((entity) => entity.entityName === clientId));
      return entry ?? false;
    });

    expect(described.values).toEqual(
      expect.arrayContaining([expect.objectContaining({ key: 'producer_byte_rate', value: 1024 })]),
    );

    await admin.alterClientQuotas({
      entries: [
        {
          entity: [{ entityType: 'client-id', entityName: clientId }],
          ops: [{ key: 'producer_byte_rate', value: 0, remove: true }],
        },
      ],
    });
    clientId = '';
  });
});
