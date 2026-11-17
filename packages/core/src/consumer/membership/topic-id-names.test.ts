import { describe, expect, it, vi } from 'vitest';
import { KafkaNonRetriableError } from '../../errors';
import { TopicIdNames, type DescribedTopic } from './topic-id-names';

const namedTopicId = Buffer.alloc(16, 1);
const matchedTopicId = Buffer.alloc(16, 2);

function createTopicIdNames({
  subscribedTopics = ['a'],
  hasPattern = false,
  described = [],
}: {
  subscribedTopics?: string[];
  hasPattern?: boolean;
  described?: DescribedTopic[];
} = {}) {
  const describeAssignedTopics = vi.fn(async () => described);
  const topicIdNames = new TopicIdNames({
    subscribedTopics: () => subscribedTopics,
    hasPattern,
    describeAssignedTopics,
  });
  return { topicIdNames, describeAssignedTopics };
}

describe('consumer/membership/topic-id-names', () => {
  it('resolves any topic id to the one subscribed topic without describing the group', async () => {
    const { topicIdNames, describeAssignedTopics } = createTopicIdNames();

    await expect(topicIdNames.resolve(namedTopicId)).resolves.toBe('a');
    expect(describeAssignedTopics).not.toHaveBeenCalled();
  });

  it('describes the group when a pattern is subscribed beside the one topic name', async () => {
    const { topicIdNames } = createTopicIdNames({
      hasPattern: true,
      described: [
        { topicId: namedTopicId, topicName: 'a' },
        { topicId: matchedTopicId, topicName: 'b-1' },
      ],
    });

    await expect(topicIdNames.resolve(matchedTopicId)).resolves.toBe('b-1');
    await expect(topicIdNames.resolve(namedTopicId)).resolves.toBe('a');
  });

  it('caches names learned from one describe', async () => {
    const { topicIdNames, describeAssignedTopics } = createTopicIdNames({
      subscribedTopics: ['a', 'b-1'],
      described: [
        { topicId: namedTopicId, topicName: 'a' },
        { topicId: matchedTopicId, topicName: 'b-1' },
      ],
    });

    await topicIdNames.resolve(namedTopicId);
    await topicIdNames.resolve(matchedTopicId);
    expect(describeAssignedTopics).toHaveBeenCalledTimes(1);
  });

  it('throws when the describe does not name the topic id', async () => {
    const { topicIdNames } = createTopicIdNames({
      subscribedTopics: [],
      described: [{ topicId: matchedTopicId, topicName: null }],
    });

    await expect(topicIdNames.resolve(matchedTopicId)).rejects.toThrow(KafkaNonRetriableError);
  });
});
