import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { createAdmin } from '../../../src/admin/index.js';
import { CONFIG_RESOURCE_TYPES } from '../../../src/protocol/enums/config-resource-types.js';
import { createCluster, createTopic, newLogger, secureRandom, waitFor } from '../../helpers/index.js';

describe('admin.configs', () => {
  let topicName: string;
  let admin: ReturnType<typeof createAdmin> | undefined;

  beforeEach(async () => {
    topicName = `test-topic-${secureRandom()}`;
    await createTopic({ topic: topicName });
  });

  afterEach(async () => {
    await admin?.disconnect();
  });

  it('describes and alters topic configs', async () => {
    const cluster = createCluster();
    admin = createAdmin({ cluster, logger: newLogger() });
    await admin.connect();

    const described = await admin.describeConfigs({
      includeSynonyms: false,
      resources: [{ type: CONFIG_RESOURCE_TYPES.TOPIC, name: topicName, configNames: ['cleanup.policy'] }],
    });

    const topicResource = described.resources.find((r) => r.resourceName === topicName);
    const cleanup = topicResource?.configEntries.find((c) => c.configName === 'cleanup.policy');
    expect(cleanup?.configValue).toBe('delete');

    await admin.alterConfigs({
      resources: [
        {
          type: CONFIG_RESOURCE_TYPES.TOPIC,
          name: topicName,
          configEntries: [{ name: 'cleanup.policy', value: 'compact' }],
        },
      ],
    });

    await waitFor(async () => {
      const after = await admin!.describeConfigs({
        includeSynonyms: false,
        resources: [{ type: CONFIG_RESOURCE_TYPES.TOPIC, name: topicName, configNames: ['cleanup.policy'] }],
      });
      const updated = after.resources
        .find((r) => r.resourceName === topicName)
        ?.configEntries.find((c) => c.configName === 'cleanup.policy');
      return updated?.configValue === 'compact' ? updated : false;
    });

    const after = await admin.describeConfigs({
      includeSynonyms: false,
      resources: [{ type: CONFIG_RESOURCE_TYPES.TOPIC, name: topicName, configNames: ['cleanup.policy'] }],
    });
    const updated = after.resources
      .find((r) => r.resourceName === topicName)
      ?.configEntries.find((c) => c.configName === 'cleanup.policy');
    expect(updated?.configValue).toBe('compact');
  });

  it('describes broker configs', async () => {
    admin = createAdmin({ cluster: createCluster(), logger: newLogger() });
    await admin.connect();
    const { brokers } = await admin.describeCluster();
    const brokerId = String(brokers[0]!.nodeId);

    const described = await admin.describeConfigs({
      includeSynonyms: false,
      resources: [{ type: CONFIG_RESOURCE_TYPES.BROKER, name: brokerId }],
    });
    expect(described.resources[0]?.configEntries.length).toBeGreaterThan(0);
  });
});
