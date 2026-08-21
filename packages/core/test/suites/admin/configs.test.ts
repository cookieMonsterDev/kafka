import { afterEach, beforeEach, describe, expect } from 'vitest';
import { createAdmin } from '../../../src/admin/index';
import { CONFIG_RESOURCE_TYPES } from '../../../src/protocol/enums/config-resource-types';
import { INCREMENTAL_ALTER_CONFIGS_OPERATIONS } from '../../../src/protocol/enums/incremental-alter-configs-operations';
import {
  createCluster,
  createTopic,
  newLogger,
  secureRandom,
  testIfKafkaAtLeast_0_11,
  testIfKafkaAtLeast_1_1,
  testIfKafkaAtLeast_2_4,
  testIfKafkaAtLeast_4_0,
  testIfKafkaAtLeast_4_1,
  waitFor,
} from '../../helpers/index';

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

  testIfKafkaAtLeast_0_11('describes and alters topic configs', async () => {
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

    const updated = await waitFor(async () => {
      const after = await admin!.describeConfigs({
        includeSynonyms: false,
        resources: [{ type: CONFIG_RESOURCE_TYPES.TOPIC, name: topicName, configNames: ['cleanup.policy'] }],
      });
      const entry = after.resources
        .find((r) => r.resourceName === topicName)
        ?.configEntries.find((c) => c.configName === 'cleanup.policy');
      return entry?.configValue === 'compact' ? entry : false;
    });
    expect(updated.configValue).toBe('compact');
  });

  testIfKafkaAtLeast_0_11('describes broker configs', async () => {
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

  testIfKafkaAtLeast_1_1('returns config synonyms when requested', async () => {
    admin = createAdmin({ cluster: createCluster(), logger: newLogger() });
    await admin.connect();

    const described = await admin.describeConfigs({
      includeSynonyms: true,
      resources: [{ type: CONFIG_RESOURCE_TYPES.TOPIC, name: topicName, configNames: ['cleanup.policy'] }],
    });
    const cleanup = described.resources
      .find((r) => r.resourceName === topicName)
      ?.configEntries.find((c) => c.configName === 'cleanup.policy');
    expect(cleanup?.configSynonyms.length).toBeGreaterThan(0);
    expect(cleanup?.configSynonyms[0]).toEqual(
      expect.objectContaining({
        configName: expect.any(String),
        configSource: expect.any(Number),
      }),
    );
  });

  testIfKafkaAtLeast_2_4('incrementally alters topic configs', async () => {
    const cluster = createCluster();
    admin = createAdmin({ cluster, logger: newLogger() });
    await admin.connect();

    await admin.incrementalAlterConfigs({
      resources: [
        {
          type: CONFIG_RESOURCE_TYPES.TOPIC,
          name: topicName,
          configs: [{ name: 'cleanup.policy', value: 'compact', operation: INCREMENTAL_ALTER_CONFIGS_OPERATIONS.SET }],
        },
      ],
    });

    const updated = await waitFor(async () => {
      const after = await admin!.describeConfigs({
        includeSynonyms: false,
        resources: [{ type: CONFIG_RESOURCE_TYPES.TOPIC, name: topicName, configNames: ['cleanup.policy'] }],
      });
      const entry = after.resources
        .find((r) => r.resourceName === topicName)
        ?.configEntries.find((c) => c.configName === 'cleanup.policy');
      return entry?.configValue === 'compact' ? entry : false;
    });
    expect(updated.configValue).toBe('compact');
  });

  testIfKafkaAtLeast_4_0('lists config resources from the controller', async () => {
    admin = createAdmin({ cluster: createCluster(), logger: newLogger() });
    await admin.connect();

    const listed = await admin.listConfigResources();
    expect(Array.isArray(listed.resources)).toBe(true);
    for (const resource of listed.resources) {
      expect(typeof resource.resourceName).toBe('string');
      expect(typeof resource.resourceType).toBe('number');
    }
  });

  testIfKafkaAtLeast_4_1('lists topic config resources by type', async () => {
    admin = createAdmin({ cluster: createCluster(), logger: newLogger() });
    await admin.connect();

    const listed = await admin.listConfigResources({
      resourceTypes: [CONFIG_RESOURCE_TYPES.TOPIC],
    });
    expect(listed.resources.every((resource) => resource.resourceType === CONFIG_RESOURCE_TYPES.TOPIC)).toBe(true);
    expect(listed.resources.some((resource) => resource.resourceName === topicName)).toBe(true);
  });
});
