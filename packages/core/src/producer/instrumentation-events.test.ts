import { describe, expect, it } from 'vitest';
import { events, REQUEST, REQUEST_QUEUE_SIZE, REQUEST_TIMEOUT, unwrap, wrap } from './instrumentation-events';

describe('producer/instrumentation-events', () => {
  it('namespaces every event under producer.*', () => {
    expect(events).toEqual({
      CONNECT: 'producer.connect',
      DISCONNECT: 'producer.disconnect',
      REQUEST: 'producer.network.request',
      REQUEST_TIMEOUT: 'producer.network.request_timeout',
      REQUEST_QUEUE_SIZE: 'producer.network.request_queue_size',
    });
  });

  it('unwraps a public producer event name to the underlying network event name', () => {
    expect(unwrap(REQUEST)).toBe('network.request');
    expect(unwrap(REQUEST_TIMEOUT)).toBe('network.request_timeout');
    expect(unwrap(REQUEST_QUEUE_SIZE)).toBe('network.request_queue_size');
  });

  it('leaves an already-unwrapped or unrelated event name untouched', () => {
    expect(unwrap(events.CONNECT)).toBe(events.CONNECT);
    expect(unwrap('something.else')).toBe('something.else');
  });

  it('wraps an underlying network event name back to its public producer event name', () => {
    expect(wrap('network.request')).toBe(REQUEST);
    expect(wrap('network.request_timeout')).toBe(REQUEST_TIMEOUT);
    expect(wrap('network.request_queue_size')).toBe(REQUEST_QUEUE_SIZE);
  });

  it('leaves an unrelated event name untouched when wrapping', () => {
    expect(wrap('something.else')).toBe('something.else');
  });

  it('round-trips wrap(unwrap(x)) back to x for every network-backed event', () => {
    for (const eventName of [REQUEST, REQUEST_TIMEOUT, REQUEST_QUEUE_SIZE]) {
      expect(wrap(unwrap(eventName))).toBe(eventName);
    }
  });
});
