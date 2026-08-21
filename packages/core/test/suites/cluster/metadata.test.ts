import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { API_KEYS } from '../../../src/protocol/requests/api-keys';
import { ZERO_TOPIC_ID } from '../../../src/protocol/requests/metadata/shared';
import { createCluster, createTopic, secureRandom, testIfKafkaAtLeast_2_8, waitFor } from '../../helpers/index';

describe('cluster.metadata', () => {
  let cluster: ReturnType<typeof createCluster> | undefined;
  let topic1: string;
  let topic2: string;
  let topic3: string;

  beforeEach(async () => {
    topic1 = `test-topic-${secureRandom()}`;
    topic2 = `test-topic-${secureRandom()}`;
    topic3 = `test-topic-${secureRandom()}`;
    await createTopic({ topic: topic1 });
    await createTopic({ topic: topic2 });
    await createTopic({ topic: topic3 });
    cluster = createCluster();
    await cluster.connect();
  });

  afterEach(async () => {
    await cluster?.disconnect();
  });

  it('returns metadata for a set of topics', async () => {
    const response = await cluster!.metadata({ topics: [topic1, topic2] });
    expect(response?.topicMetadata).toHaveLength(2);
    expect(response?.topicMetadata.map((t) => t.topic).sort()).toEqual([topic1, topic2].sort());
  });

  it('returns metadata for all topics', async () => {
    const expected = [topic1, topic2, topic3];
    const response = await waitFor(async () => {
      const metadata = await cluster!.metadata({ topics: [] });
      if (metadata == null) return false;
      const names = metadata.topicMetadata.map((t) => t.topic);
      return expected.every((topic) => names.includes(topic)) ? metadata : false;
    });
    expect(response.topicMetadata.length).toBeGreaterThanOrEqual(3);
    expect(response.topicMetadata.map((t) => t.topic)).toEqual(expect.arrayContaining(expected));
  });

  it('adds target topics and finds partition metadata', async () => {
    await cluster!.addTargetTopic(topic1);
    const partitions = cluster!.findTopicPartitionMetadata(topic1);
    expect(partitions.length).toBeGreaterThan(0);
    expect(cluster!.getNodeIds().length).toBe(3);
  });

  testIfKafkaAtLeast_2_8('returns a 16-byte topicId for a real topic (KIP-516)', async () => {
    const response = await cluster!.metadata({ topics: [topic1] });
    const topic = response?.topicMetadata.find((entry) => entry.topic === topic1);
    expect(topic?.topicId).toBeInstanceOf(Buffer);
    expect(topic?.topicId?.length).toBe(16);

    const negotiated = cluster!.brokerPool.versions?.[API_KEYS.Metadata]?.maxVersion ?? 0;
    const clientMax = 13;
    const overlap = Math.min(negotiated, clientMax);
    if (overlap >= 12) {
      expect(topic?.topicId?.equals(ZERO_TOPIC_ID)).toBe(false);
    }
  });
});
