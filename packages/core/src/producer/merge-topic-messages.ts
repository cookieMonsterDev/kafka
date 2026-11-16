import type { Message, TopicMessages } from './types';

/**
 * Merges entries that name the same topic into one, keeping topics in first-seen order and messages
 * in order. Always returns new message arrays, so later changes to the caller's arrays don't reach
 * records that wait to be sent.
 */
export function mergeTopicMessages(topicMessages: readonly TopicMessages[]): TopicMessages[] {
  const [onlyTopic] = topicMessages;
  if (topicMessages.length === 1 && onlyTopic) return [{ topic: onlyTopic.topic, messages: [...onlyTopic.messages] }];

  const mergedByTopic = new Map<string, Message[]>();
  for (const { topic, messages } of topicMessages) {
    const current = mergedByTopic.get(topic);
    if (current) {
      current.push(...messages);
    } else {
      mergedByTopic.set(topic, [...messages]);
    }
  }

  return [...mergedByTopic.entries()].map(([topic, messages]) => ({ topic, messages }));
}
