import { describe, expect, it, vi } from 'vitest';
import { InstrumentationEventEmitter } from './emitter.js';

interface TestEventMap {
  'consumer.crash': { error: Error };
  'consumer.group_join': { memberId: string };
}

describe('instrumentation/InstrumentationEventEmitter', () => {
  it('delivers the payload wrapped in an InstrumentationEvent', () => {
    const emitter = new InstrumentationEventEmitter<TestEventMap>();
    const listener = vi.fn();

    emitter.addListener('consumer.group_join', listener);
    emitter.emit('consumer.group_join', { memberId: 'member-1' });

    expect(listener).toHaveBeenCalledTimes(1);
    const [event] = listener.mock.calls[0]!;
    expect(event.type).toBe('consumer.group_join');
    expect(event.payload).toEqual({ memberId: 'member-1' });
  });

  it('does not construct an event when there are no listeners', () => {
    const emitter = new InstrumentationEventEmitter<TestEventMap>();
    // No listener registered; emit should be a no-op that doesn't throw.
    expect(() => emitter.emit('consumer.crash', { error: new Error('boom') })).not.toThrow();
  });

  it('removeListener stops further delivery', () => {
    const emitter = new InstrumentationEventEmitter<TestEventMap>();
    const listener = vi.fn();

    const removeListener = emitter.addListener('consumer.group_join', listener);
    removeListener();
    emitter.emit('consumer.group_join', { memberId: 'member-1' });

    expect(listener).not.toHaveBeenCalled();
  });

  it('propagates a synchronous listener error to the caller of emit', () => {
    const emitter = new InstrumentationEventEmitter<TestEventMap>();
    const failing = vi.fn(() => {
      throw new Error('listener blew up');
    });
    emitter.addListener('consumer.group_join', failing);

    expect(() => emitter.emit('consumer.group_join', { memberId: 'member-1' })).toThrow('listener blew up');
    expect(failing).toHaveBeenCalledTimes(1);
  });

  it('throws for an empty event name', () => {
    const emitter = new InstrumentationEventEmitter<TestEventMap>();
    // @ts-expect-error exercising the runtime guard against an empty event name
    expect(() => emitter.emit('', {})).toThrow('Invalid event name');
  });
});
