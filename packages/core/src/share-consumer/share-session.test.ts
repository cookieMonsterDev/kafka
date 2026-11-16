import { describe, expect, it } from 'vitest';
import { SHARE_SESSION_INITIAL_EPOCH } from '../protocol/requests/share-fetch/index';
import { ShareSessionHandler, type ShareSessionTopic } from './share-session';

const EVENTS_ID = Buffer.alloc(16, 1);
const CLICKS_ID = Buffer.alloc(16, 2);

function events(...partitions: number[]): ShareSessionTopic {
  return { topic: 'events', topicId: EVENTS_ID, partitions };
}

function clicks(...partitions: number[]): ShareSessionTopic {
  return { topic: 'clicks', topicId: CLICKS_ID, partitions };
}

describe('share-consumer/share-session', () => {
  it('opens a session at the initial epoch with nothing to forget', () => {
    const session = new ShareSessionHandler();

    expect(session.hasSession()).toBe(false);
    expect(session.buildRequest([events(0, 1)])).toEqual({
      shareSessionEpoch: SHARE_SESSION_INITIAL_EPOCH,
      forgottenTopics: [],
    });
  });

  it('moves to the next epoch each time the broker accepts a request', () => {
    const session = new ShareSessionHandler();
    session.buildRequest([events(0)]);
    session.handleResponse();

    expect(session.hasSession()).toBe(true);
    expect(session.buildRequest([events(0)]).shareSessionEpoch).toBe(1);
    session.handleResponse();
    expect(session.buildRequest([events(0)]).shareSessionEpoch).toBe(2);
  });

  it('forgets partitions and topics the next request no longer names', () => {
    const session = new ShareSessionHandler();
    session.buildRequest([events(0, 1, 2), clicks(0)]);
    session.handleResponse();

    expect(session.buildRequest([events(1)]).forgottenTopics).toEqual([
      { topicId: EVENTS_ID, partitions: [0, 2] },
      { topicId: CLICKS_ID, partitions: [0] },
    ]);
  });

  it('keeps forgetting against what the broker holds until a request is accepted', () => {
    const session = new ShareSessionHandler();
    session.buildRequest([events(0, 1)]);
    session.handleResponse();

    // The first request after the shrink failed, so the broker still holds partition 1.
    session.buildRequest([events(0)]);
    expect(session.buildRequest([events(0)]).forgottenTopics).toEqual([{ topicId: EVENTS_ID, partitions: [1] }]);
    session.handleResponse();
    expect(session.buildRequest([events(0)]).forgottenTopics).toEqual([]);
  });

  it('opens a new session after a reset', () => {
    const session = new ShareSessionHandler();
    session.buildRequest([events(0, 1)]);
    session.handleResponse();
    session.reset();

    expect(session.hasSession()).toBe(false);
    expect(session.buildRequest([events(0)])).toEqual({
      shareSessionEpoch: SHARE_SESSION_INITIAL_EPOCH,
      forgottenTopics: [],
    });
    session.handleResponse();
    expect(session.buildRequest([events(0)]).shareSessionEpoch).toBe(1);
  });
});
