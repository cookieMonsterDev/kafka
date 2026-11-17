import { describe, expect, it } from 'vitest';
import { mergeTopicMessages } from './merge-topic-messages';

describe('producer/mergeTopicMessages', () => {
  it('merges entries for the same topic, keeping first-seen topic order and message order', () => {
    const merged = mergeTopicMessages([
      { topic: 'a', messages: [{ value: '1' }] },
      { topic: 'b', messages: [{ value: '2' }] },
      { topic: 'a', messages: [{ value: '3' }, { value: '4' }] },
    ]);

    expect(merged).toEqual([
      { topic: 'a', messages: [{ value: '1' }, { value: '3' }, { value: '4' }] },
      { topic: 'b', messages: [{ value: '2' }] },
    ]);
  });

  it("copies a lone topic's messages, so later changes to the caller's array don't reach it", () => {
    const messages = [{ value: '1' }];
    const [merged] = mergeTopicMessages([{ topic: 'a', messages }]);
    messages.push({ value: '2' });

    expect(merged?.messages).toEqual([{ value: '1' }]);
  });
});
