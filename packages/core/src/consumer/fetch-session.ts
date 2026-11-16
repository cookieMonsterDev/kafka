import type { FetchPartitionRequest, FetchTopicRequest, ForgottenTopic } from '../protocol/requests/fetch/shared';

/** KIP-227: epoch 0 both opens a session and marks a client that never gets one (full fetch every time). */
export const FETCH_SESSION_INITIAL_EPOCH = 0;
/** KIP-227: epoch -1 asks the broker to close an open session. */
export const FETCH_SESSION_FINAL_EPOCH = -1;

/** The highest epoch a session reaches before it wraps around. */
const MAX_SESSION_EPOCH = 0x7fffffff;

/**
 * The epoch after `epoch` in a KIP-227 fetch session or a KIP-932 share session. It wraps to 1,
 * not 0, because epoch 0 opens a new session.
 */
export function nextSessionEpoch(epoch: number): number {
  return epoch === MAX_SESSION_EPOCH ? 1 : epoch + 1;
}

interface SessionPartitionState {
  currentLeaderEpoch: number;
  fetchOffset: bigint;
  lastFetchedEpoch: number;
  logStartOffset: bigint;
  maxBytes: number;
}

interface SessionTopicState {
  topicId?: Buffer;
  partitions: Map<number, SessionPartitionState>;
}

export interface FetchSessionRequest {
  sessionId: number;
  sessionEpoch: number;
  topics: FetchTopicRequest[];
  forgottenTopics: ForgottenTopic[];
}

function toState(partition: FetchPartitionRequest): SessionPartitionState {
  return {
    currentLeaderEpoch: partition.currentLeaderEpoch ?? -1,
    fetchOffset: partition.fetchOffset,
    lastFetchedEpoch: partition.lastFetchedEpoch ?? -1,
    logStartOffset: partition.logStartOffset ?? -1n,
    maxBytes: partition.maxBytes,
  };
}

function statesEqual(a: SessionPartitionState, b: SessionPartitionState): boolean {
  return (
    a.currentLeaderEpoch === b.currentLeaderEpoch &&
    a.fetchOffset === b.fetchOffset &&
    a.lastFetchedEpoch === b.lastFetchedEpoch &&
    a.logStartOffset === b.logStartOffset &&
    a.maxBytes === b.maxBytes
  );
}

function toTopicMap(desired: readonly FetchTopicRequest[]): Map<string, SessionTopicState> {
  const map = new Map<string, SessionTopicState>();
  for (const { topic, topicId, partitions } of desired) {
    const partitionStates = new Map<number, SessionPartitionState>();
    for (const partition of partitions) partitionStates.set(partition.partition, toState(partition));
    map.set(topic, { topicId, partitions: partitionStates });
  }
  return map;
}

/**
 * Per-broker KIP-227 incremental fetch session. The first request establishes the session with
 * the full desired partition set (`sessionId: 0, sessionEpoch: 0`). Once the broker grants a
 * session id, later requests only carry partitions whose fetch parameters changed since the last
 * request (`topics`) and partitions dropped from the desired set (`forgottenTopics`) — everything
 * else stays implicit on the broker side.
 *
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-227%3A+Introduce+Incremental+FetchRequests+to+Increase+Partition+Scalability
 */
export class FetchSessionHandler {
  #sessionId = 0;
  #epoch = FETCH_SESSION_INITIAL_EPOCH;
  #topics = new Map<string, SessionTopicState>();

  hasSession(): boolean {
    return this.#sessionId !== 0;
  }

  /** Build the next Fetch request patch for the full set of topic-partitions the caller wants. */
  buildRequest(desired: readonly FetchTopicRequest[]): FetchSessionRequest {
    if (!this.hasSession()) {
      this.#topics = toTopicMap(desired);
      return { sessionId: 0, sessionEpoch: FETCH_SESSION_INITIAL_EPOCH, topics: [...desired], forgottenTopics: [] };
    }

    const desiredTopics = toTopicMap(desired);
    const topics: FetchTopicRequest[] = [];
    const forgottenTopics: ForgottenTopic[] = [];

    for (const { topic, topicId, partitions } of desired) {
      const previous = this.#topics.get(topic);
      const changed = partitions.filter((partitionRequest) => {
        const previousState = previous?.partitions.get(partitionRequest.partition);
        return !previousState || !statesEqual(previousState, toState(partitionRequest));
      });
      if (changed.length > 0) topics.push({ topic, topicId, partitions: changed });
    }

    for (const [topic, previous] of this.#topics) {
      const nextPartitions = desiredTopics.get(topic)?.partitions;
      const removedPartitions = [...previous.partitions.keys()].filter((partition) => !nextPartitions?.has(partition));
      if (removedPartitions.length > 0) {
        forgottenTopics.push({ topic, topicId: previous.topicId, partitions: removedPartitions });
      }
    }

    this.#topics = desiredTopics;
    this.#epoch = nextSessionEpoch(this.#epoch);
    return { sessionId: this.#sessionId, sessionEpoch: this.#epoch, topics, forgottenTopics };
  }

  /** Adopt the session id the broker returned; 0 means it didn't grant one (stay sessionless). */
  handleResponse(sessionId: number): void {
    this.#sessionId = sessionId;
  }

  /** Drop all local state so the next `buildRequest` starts a fresh full fetch. */
  reset(): void {
    this.#sessionId = 0;
    this.#epoch = FETCH_SESSION_INITIAL_EPOCH;
    this.#topics = new Map();
  }

  /** A final, empty incremental request telling the broker to evict the session now, or `null` if none is open. */
  closeRequest(): FetchSessionRequest | null {
    if (!this.hasSession()) return null;
    return { sessionId: this.#sessionId, sessionEpoch: FETCH_SESSION_FINAL_EPOCH, topics: [], forgottenTopics: [] };
  }
}
