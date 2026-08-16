import { describe, expect, it } from 'vitest';
import { SubscriptionState } from './subscription-state';

const byTopic = (a: { topic: string }, b: { topic: string }): number => a.topic.localeCompare(b.topic);

describe('consumer/subscription-state pause / resume', () => {
  it('pauses the selected topics', () => {
    const subscriptionState = new SubscriptionState();
    subscriptionState.pause([{ topic: 'topic1' }, { topic: 'topic2' }]);
    expect(subscriptionState.paused().sort(byTopic)).toEqual([]);

    subscriptionState.assign([
      { topic: 'topic1', partitions: [0, 1] },
      { topic: 'topic2', partitions: [1, 2] },
    ]);
    expect(subscriptionState.paused().sort(byTopic)).toEqual([
      { topic: 'topic1', partitions: [0, 1] },
      { topic: 'topic2', partitions: [1, 2] },
    ]);
  });

  it('resumes the selected topics', () => {
    const subscriptionState = new SubscriptionState();
    subscriptionState.pause([{ topic: 'topic1' }, { topic: 'topic2' }]);
    subscriptionState.assign([
      { topic: 'topic1', partitions: [0, 1] },
      { topic: 'topic2', partitions: [1, 2] },
    ]);
    subscriptionState.resume([{ topic: 'topic2' }]);

    expect(subscriptionState.paused().sort(byTopic)).toEqual([{ topic: 'topic1', partitions: [0, 1] }]);
  });

  it('pauses the selected partitions', () => {
    const subscriptionState = new SubscriptionState();
    subscriptionState.pause([{ topic: 'topic1', partitions: [0, 1] }]);
    expect(subscriptionState.paused()).toEqual([]);

    subscriptionState.assign([{ topic: 'topic1', partitions: [0, 1, 2, 3] }]);
    expect(subscriptionState.paused()).toEqual([{ topic: 'topic1', partitions: [0, 1] }]);

    subscriptionState.pause([{ topic: 'topic1', partitions: [1, 2] }]);
    expect(subscriptionState.paused()).toEqual([{ topic: 'topic1', partitions: [0, 1, 2] }]);

    subscriptionState.pause([{ topic: 'topic1', partitions: [4] }]);
    expect(subscriptionState.paused()).toEqual([{ topic: 'topic1', partitions: [0, 1, 2] }]);
  });

  it('resumes the selected partitions', () => {
    const subscriptionState = new SubscriptionState();
    subscriptionState.pause([{ topic: 'topic1', partitions: [0, 1] }]);
    subscriptionState.assign([{ topic: 'topic1', partitions: [0, 1, 2, 3] }]);
    subscriptionState.resume([{ topic: 'topic1', partitions: [1] }]);
    expect(subscriptionState.paused()).toEqual([{ topic: 'topic1', partitions: [0] }]);

    subscriptionState.resume([{ topic: 'topic1', partitions: [4] }]);
    expect(subscriptionState.paused()).toEqual([{ topic: 'topic1', partitions: [0] }]);

    subscriptionState.pause([{ topic: 'topic1' }]);
    subscriptionState.resume([{ topic: 'topic1', partitions: [1] }]);
    expect(subscriptionState.paused()).toEqual([{ topic: 'topic1', partitions: [0, 2, 3] }]);
  });
});

describe('consumer/subscription-state isPaused', () => {
  it('can determine whether a topic partition is paused', () => {
    const subscriptionState = new SubscriptionState();
    subscriptionState.pause([{ topic: 'topic1', partitions: [0, 1] }, { topic: 'topic2' }]);

    expect(subscriptionState.isPaused('topic1', 0)).toEqual(true);
    expect(subscriptionState.isPaused('topic1', 2)).toEqual(false);
    expect(subscriptionState.isPaused('topic2', 0)).toEqual(true);
    expect(subscriptionState.isPaused('topic2', 2)).toEqual(true);
    expect(subscriptionState.isPaused('unknown', 0)).toEqual(false);
  });
});

describe('consumer/subscription-state assignments', () => {
  it('can track assigned partitions per topic', () => {
    const subscriptionState = new SubscriptionState();
    subscriptionState.assign([{ topic: 'topic1', partitions: [0, 1] }]);
    expect(subscriptionState.active()).toEqual([{ topic: 'topic1', partitions: [0, 1] }]);

    subscriptionState.assign([{ topic: 'topic2', partitions: [3, 4] }]);
    expect(subscriptionState.active()).toEqual([{ topic: 'topic2', partitions: [3, 4] }]);
  });

  it('can return which topic partitions are assigned and not paused', () => {
    const subscriptionState = new SubscriptionState();
    subscriptionState.assign([{ topic: 'topic1', partitions: [0, 1] }]);
    subscriptionState.pause([{ topic: 'topic1', partitions: [0] }]);

    expect(subscriptionState.active()).toEqual([{ topic: 'topic1', partitions: [1] }]);

    subscriptionState.pause([{ topic: 'topic2' }]);
    subscriptionState.assign([{ topic: 'topic2', partitions: [0, 1, 2, 6, 7] }]);
    expect(subscriptionState.active()).toEqual([{ topic: 'topic2', partitions: [] }]);
  });

  it('returns assigned partitions independently of pause state', () => {
    const subscriptionState = new SubscriptionState();
    subscriptionState.assign([{ topic: 'topic1', partitions: [0, 1] }]);
    subscriptionState.pause([{ topic: 'topic1' }]);

    expect(subscriptionState.assigned()).toEqual([{ topic: 'topic1', partitions: [0, 1] }]);
    expect(subscriptionState.active()).toEqual([{ topic: 'topic1', partitions: [] }]);
  });

  it('treats empty pause, resume, and assign as no-ops', () => {
    const subscriptionState = new SubscriptionState();
    subscriptionState.assign();
    subscriptionState.pause();
    subscriptionState.resume();

    expect(subscriptionState.assigned()).toEqual([]);
    expect(subscriptionState.paused()).toEqual([]);
    expect(subscriptionState.active()).toEqual([]);
  });
});
