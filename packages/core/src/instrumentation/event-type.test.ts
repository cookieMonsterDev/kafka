import { describe, expect, it } from 'vitest';
import { namespace } from './event-type';

describe('instrumentation/eventType', () => {
  it('joins namespace and type with a dot', () => {
    const consumerEvent = namespace('consumer');
    expect(consumerEvent('group_join')).toBe('consumer.group_join');
    expect(consumerEvent('crash')).toBe('consumer.crash');
  });
});
