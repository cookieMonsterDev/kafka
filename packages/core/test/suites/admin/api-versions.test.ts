import { afterEach, describe, expect, it } from 'vitest';
import { createAdmin } from '../../../src/admin/index';
import type { DescribeApiVersionsOptions } from '../../../src/admin/types';
import { API_KEYS } from '../../../src/protocol/requests/api-keys';
import { createCluster, newLogger, testIfKafkaAtLeast_4_4, testIfKafkaAtMost_0_10 } from '../../helpers/index';

describe('admin.describeApiVersions', () => {
  let admin: ReturnType<typeof createAdmin> | undefined;

  afterEach(async () => {
    await admin?.disconnect();
  });

  async function describeApiVersions(options?: DescribeApiVersionsOptions) {
    admin = createAdmin({ cluster: createCluster(), logger: newLogger() });
    await admin.connect();
    return admin.describeApiVersions(options);
  }

  async function describeClusterNodes() {
    const describer = createAdmin({ cluster: createCluster(), logger: newLogger() });
    await describer.connect();
    try {
      return (await describer.describeCluster()).brokers;
    } finally {
      await describer.disconnect();
    }
  }

  it('describes every broker, with a negotiated version inside each advertised range', async () => {
    const nodes = await describeClusterNodes();
    const { brokers } = await describeApiVersions();

    expect(brokers.map(({ nodeId }) => nodeId).sort()).toEqual(nodes.map(({ nodeId }) => nodeId).sort());
    for (const broker of brokers) {
      expect(broker.host).toEqual(expect.any(String));
      expect(broker.port).toBeGreaterThan(0);
      const produce = broker.apis.find(({ apiKey }) => apiKey === API_KEYS.Produce);
      expect(produce).toMatchObject({ apiName: 'Produce', isImplemented: true, version: expect.any(Number) });

      for (const api of broker.apis) {
        if (api.version === undefined) continue;
        expect(api.isImplemented).toBe(true);
        expect(api.version).toBeGreaterThanOrEqual(api.brokerVersions?.minVersion ?? Infinity);
        expect(api.version).toBeLessThanOrEqual(api.brokerVersions?.maxVersion ?? -Infinity);
      }
    }
  });

  it('describes only the requested broker', async () => {
    const nodes = await describeClusterNodes();
    const nodeId = nodes[0]?.nodeId ?? 0;

    const { brokers } = await describeApiVersions({ brokerIds: [nodeId] });

    expect(brokers.map((broker) => broker.nodeId)).toEqual([nodeId]);
  });

  testIfKafkaAtLeast_4_4('reports ApiVersions v5 and APIs this client does not implement', async () => {
    const { brokers } = await describeApiVersions();
    const apis = brokers[0]?.apis ?? [];

    expect(apis.find(({ apiKey }) => apiKey === API_KEYS.ApiVersions)?.version).toBe(5);
    expect(apis.find(({ apiKey }) => apiKey === API_KEYS.StreamsGroupHeartbeat)).toMatchObject({
      apiName: 'StreamsGroupHeartbeat',
      brokerVersions: expect.any(Object),
      isImplemented: false,
    });
  });

  testIfKafkaAtMost_0_10('negotiates the versions a Kafka 0.10 broker supports', async () => {
    const { brokers } = await describeApiVersions();
    const apis = brokers[0]?.apis ?? [];

    expect(apis.find(({ apiKey }) => apiKey === API_KEYS.Produce)?.version).toBe(2);
    expect(apis.find(({ apiKey }) => apiKey === API_KEYS.DescribeCluster)).toMatchObject({ isImplemented: true });
    expect(apis.find(({ apiKey }) => apiKey === API_KEYS.DescribeCluster)?.brokerVersions).toBeUndefined();
  });
});
