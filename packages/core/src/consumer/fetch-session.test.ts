import { describe, expect, it } from 'vitest';
import type { FetchTopicRequest } from '../protocol/requests/fetch/shared';
import { FETCH_SESSION_FINAL_EPOCH, FETCH_SESSION_INITIAL_EPOCH, FetchSessionHandler } from './fetch-session';

const topicId = Buffer.alloc(16, 1);

function topic(
  partitions: { partition: number; fetchOffset: bigint; currentLeaderEpoch?: number }[],
): FetchTopicRequest[] {
  return [
    {
      topic: 'topic1',
      topicId,
      partitions: partitions.map(({ partition, fetchOffset, currentLeaderEpoch }) => ({
        partition,
        fetchOffset,
        currentLeaderEpoch,
        maxBytes: 1024,
      })),
    },
  ];
}

describe('consumer/fetch-session', () => {
  it('sends the full partition set with sessionId 0 before a session is established', () => {
    const handler = new FetchSessionHandler();
    const desired = topic([{ partition: 0, fetchOffset: 100n }]);

    const request = handler.buildRequest(desired);

    expect(request).toEqual({
      sessionId: 0,
      sessionEpoch: FETCH_SESSION_INITIAL_EPOCH,
      topics: desired,
      forgottenTopics: [],
    });
    expect(handler.hasSession()).toBe(false);
  });

  it('adopts the broker-granted session id and sends nothing further for unchanged partitions', () => {
    const handler = new FetchSessionHandler();
    const desired = topic([{ partition: 0, fetchOffset: 100n }]);

    handler.buildRequest(desired);
    handler.handleResponse(42);
    expect(handler.hasSession()).toBe(true);

    const secondRequest = handler.buildRequest(desired);

    expect(secondRequest).toEqual({ sessionId: 42, sessionEpoch: 1, topics: [], forgottenTopics: [] });
  });

  it('sends only the partition whose fetch offset advanced', () => {
    const handler = new FetchSessionHandler();
    handler.buildRequest(
      topic([
        { partition: 0, fetchOffset: 100n },
        { partition: 1, fetchOffset: 200n },
      ]),
    );
    handler.handleResponse(42);

    const request = handler.buildRequest(
      topic([
        { partition: 0, fetchOffset: 150n },
        { partition: 1, fetchOffset: 200n },
      ]),
    );

    expect(request.sessionId).toBe(42);
    expect(request.sessionEpoch).toBe(1);
    expect(request.topics).toEqual([
      {
        topic: 'topic1',
        topicId,
        partitions: [{ partition: 0, fetchOffset: 150n, currentLeaderEpoch: undefined, maxBytes: 1024 }],
      },
    ]);
    expect(request.forgottenTopics).toEqual([]);
  });

  it('sends a newly added partition and forgets a removed one in the same request', () => {
    const handler = new FetchSessionHandler();
    handler.buildRequest(topic([{ partition: 0, fetchOffset: 100n }]));
    handler.handleResponse(42);

    const request = handler.buildRequest(topic([{ partition: 1, fetchOffset: 500n }]));

    expect(request.topics).toEqual([
      {
        topic: 'topic1',
        topicId,
        partitions: [{ partition: 1, fetchOffset: 500n, currentLeaderEpoch: undefined, maxBytes: 1024 }],
      },
    ]);
    expect(request.forgottenTopics).toEqual([{ topic: 'topic1', topicId, partitions: [0] }]);
  });

  it('treats a changed currentLeaderEpoch as a partition update', () => {
    const handler = new FetchSessionHandler();
    handler.buildRequest(topic([{ partition: 0, fetchOffset: 100n, currentLeaderEpoch: 3 }]));
    handler.handleResponse(42);

    const request = handler.buildRequest(topic([{ partition: 0, fetchOffset: 100n, currentLeaderEpoch: 4 }]));

    expect(request.topics).toHaveLength(1);
    expect(request.topics[0]?.partitions).toEqual([
      { partition: 0, fetchOffset: 100n, currentLeaderEpoch: 4, maxBytes: 1024 },
    ]);
  });

  it('keeps sending the full partition set when the broker never grants a session (sessionless brokers)', () => {
    const handler = new FetchSessionHandler();
    const desired = topic([{ partition: 0, fetchOffset: 100n }]);

    handler.buildRequest(desired);
    handler.handleResponse(0);
    expect(handler.hasSession()).toBe(false);

    const secondRequest = handler.buildRequest(desired);

    expect(secondRequest).toEqual({
      sessionId: 0,
      sessionEpoch: FETCH_SESSION_INITIAL_EPOCH,
      topics: desired,
      forgottenTopics: [],
    });
  });

  it('resets to a fresh full fetch after reset()', () => {
    const handler = new FetchSessionHandler();
    handler.buildRequest(topic([{ partition: 0, fetchOffset: 100n }]));
    handler.handleResponse(42);

    handler.reset();

    expect(handler.hasSession()).toBe(false);
    const request = handler.buildRequest(topic([{ partition: 0, fetchOffset: 100n }]));
    expect(request.sessionId).toBe(0);
    expect(request.sessionEpoch).toBe(FETCH_SESSION_INITIAL_EPOCH);
    expect(request.topics).toHaveLength(1);
  });

  it('returns null from closeRequest when no session is open', () => {
    const handler = new FetchSessionHandler();
    expect(handler.closeRequest()).toBeNull();
  });

  it('builds a final-epoch close request for an open session', () => {
    const handler = new FetchSessionHandler();
    handler.buildRequest(topic([{ partition: 0, fetchOffset: 100n }]));
    handler.handleResponse(42);

    expect(handler.closeRequest()).toEqual({
      sessionId: 42,
      sessionEpoch: FETCH_SESSION_FINAL_EPOCH,
      topics: [],
      forgottenTopics: [],
    });
  });
});
