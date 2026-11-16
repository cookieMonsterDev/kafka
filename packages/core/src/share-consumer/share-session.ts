import {
  SHARE_SESSION_INITIAL_EPOCH,
  type ShareFetchForgottenTopicInput,
} from '../protocol/requests/share-fetch/index';

const MAX_EPOCH = 0x7fffffff;

/** A topic's partitions that one node leads, with the topic id ShareFetch addresses the topic by. */
export interface ShareSessionTopic {
  topic: string;
  topicId: Buffer;
  partitions: readonly number[];
}

export interface ShareSessionRequest {
  shareSessionEpoch: number;
  forgottenTopics: ShareFetchForgottenTopicInput[];
}

interface SessionTopicState {
  topicId: Buffer;
  partitions: Set<number>;
}

function toTopicMap(topics: readonly ShareSessionTopic[]): Map<string, SessionTopicState> {
  const map = new Map<string, SessionTopicState>();
  for (const { topic, topicId, partitions } of topics) map.set(topic, { topicId, partitions: new Set(partitions) });
  return map;
}

/**
 * One node's KIP-932 share session, modelled on the classic consumer's `FetchSessionHandler`.
 * Epoch 0 opens the session with the partitions the request names. Each ShareFetch the broker
 * accepts moves it to the next epoch, and partitions the session held that a later request no
 * longer names are sent as forgotten topics.
 *
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-932%3A+Queues+for+Kafka
 */
export class ShareSessionHandler {
  #epoch = SHARE_SESSION_INITIAL_EPOCH;
  /** The partitions the broker's session holds. */
  #topics = new Map<string, SessionTopicState>();
  /** The partitions of the last built request, which the session holds once the broker accepts it. */
  #requestedTopics = new Map<string, SessionTopicState>();

  /** Whether a session is open, so the next request continues it instead of opening one. */
  hasSession(): boolean {
    return this.#epoch !== SHARE_SESSION_INITIAL_EPOCH;
  }

  /** Builds the session fields of the next ShareFetch for the full set of partitions the caller wants. */
  buildRequest(topics: readonly ShareSessionTopic[]): ShareSessionRequest {
    this.#requestedTopics = toTopicMap(topics);
    if (!this.hasSession()) return { shareSessionEpoch: SHARE_SESSION_INITIAL_EPOCH, forgottenTopics: [] };

    const forgottenTopics: ShareFetchForgottenTopicInput[] = [];
    for (const [topic, { topicId, partitions }] of this.#topics) {
      const requestedPartitions = this.#requestedTopics.get(topic)?.partitions;
      const removed: number[] = [];
      for (const partition of partitions) {
        if (!requestedPartitions?.has(partition)) removed.push(partition);
      }
      if (removed.length > 0) forgottenTopics.push({ topicId, partitions: removed });
    }
    return { shareSessionEpoch: this.#epoch, forgottenTopics };
  }

  /** The broker accepted the last built request: the session now holds its partitions at the next epoch. */
  handleResponse(): void {
    this.#topics = this.#requestedTopics;
    this.#epoch = this.#epoch === MAX_EPOCH ? 1 : this.#epoch + 1;
  }

  /** Drops the session so the next request opens a new one at epoch 0. */
  reset(): void {
    this.#epoch = SHARE_SESSION_INITIAL_EPOCH;
    this.#topics = new Map();
  }
}
