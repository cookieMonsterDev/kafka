import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { createCluster, createTopic, secureRandom } from '../../helpers/index.js';

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
    const response = await cluster!.metadata({ topics: [] });
    expect(response?.topicMetadata.length).toBeGreaterThanOrEqual(3);
    expect(response?.topicMetadata.map((t) => t.topic)).toEqual(expect.arrayContaining([topic1, topic2, topic3]));
  });

  it('adds target topics and finds partition metadata', async () => {
    await cluster!.addTargetTopic(topic1);
    const partitions = cluster!.findTopicPartitionMetadata(topic1);
    expect(partitions.length).toBeGreaterThan(0);
    expect(cluster!.getNodeIds().length).toBe(3);
  });
});
