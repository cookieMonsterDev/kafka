import { describe, expect, it } from 'vitest';
import { InstrumentationEvent } from './event';

describe('instrumentation/InstrumentationEvent', () => {
  it('carries the type, payload and a timestamp', () => {
    const before = Date.now();
    const event = new InstrumentationEvent('my.event', { foo: 'bar' });
    const after = Date.now();

    expect(event.type).toBe('my.event');
    expect(event.payload).toEqual({ foo: 'bar' });
    expect(event.timestamp).toBeGreaterThanOrEqual(before);
    expect(event.timestamp).toBeLessThanOrEqual(after);
  });

  it('assigns increasing ids across instances', () => {
    const first = new InstrumentationEvent('a', null);
    const second = new InstrumentationEvent('b', null);
    expect(second.id).toBeGreaterThan(first.id);
  });
});
