import { ZERO_TOPIC_ID } from './metadata/shared';

/** True when `topicId` is a non-zero 16-byte UUID the broker can look up. */
export function isUsableTopicId(topicId: Buffer | undefined): topicId is Buffer {
  return topicId != null && topicId.length === 16 && !topicId.equals(ZERO_TOPIC_ID);
}

/**
 * The name of the requested topic that a topic-id response entry answers (KIP-516): the requested
 * topic with that id, else the one at the same index, else an empty string.
 */
export function requestedTopicName(
  topicId: Buffer,
  index: number,
  requestedTopics: readonly { topic: string; topicId?: Buffer | undefined }[],
): string {
  const byId = requestedTopics.find((entry) => entry.topicId != null && entry.topicId.equals(topicId));
  if (byId) return byId.topic;
  return requestedTopics[index]?.topic ?? '';
}

/**
 * `topicId` when the broker can look it up. A topic-id request version calls this while it is
 * encoded, not when it is built: `Broker#send` builds the negotiated version's request before
 * `preferVersion` can pick a name-based version for a topic without an id.
 */
export function requireTopicId(
  apiName: string,
  apiVersion: number,
  { topic, topicId }: { topic: string; topicId?: Buffer | undefined },
): Buffer {
  if (!isUsableTopicId(topicId)) {
    throw new RangeError(`${apiName} v${apiVersion} requires a 16-byte topicId for topic ${topic}`);
  }
  return topicId;
}
