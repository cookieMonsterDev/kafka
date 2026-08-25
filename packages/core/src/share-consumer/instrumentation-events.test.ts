import { describe, expect, it } from 'vitest';
import {
  ACKNOWLEDGE,
  events,
  FETCH,
  FETCH_START,
  REQUEST,
  REQUEST_QUEUE_SIZE,
  REQUEST_TIMEOUT,
  unwrap,
  wrap,
} from './instrumentation-events';

describe('share-consumer/instrumentation-events', () => {
  it('namespaces every event under share_consumer.*', () => {
    expect(events).toEqual({
      FETCH_START,
      FETCH,
      ACKNOWLEDGE,
      REQUEST,
      REQUEST_TIMEOUT,
      REQUEST_QUEUE_SIZE,
    });
    expect(FETCH_START).toBe('share_consumer.fetch_start');
    expect(FETCH).toBe('share_consumer.fetch');
    expect(ACKNOWLEDGE).toBe('share_consumer.acknowledge');
    expect(REQUEST).toBe('share_consumer.network.request');
  });

  it('unwraps a public share consumer event name to the underlying network event name', () => {
    expect(unwrap(REQUEST)).toBe('network.request');
    expect(unwrap(REQUEST_TIMEOUT)).toBe('network.request_timeout');
    expect(unwrap(REQUEST_QUEUE_SIZE)).toBe('network.request_queue_size');
  });

  it('leaves an already-unwrapped or unrelated event name untouched', () => {
    expect(unwrap(events.FETCH)).toBe(events.FETCH);
    expect(unwrap('something.else')).toBe('something.else');
  });

  it('wraps an underlying network event name back to its public share consumer event name', () => {
    expect(wrap('network.request')).toBe(REQUEST);
    expect(wrap('network.request_timeout')).toBe(REQUEST_TIMEOUT);
    expect(wrap('network.request_queue_size')).toBe(REQUEST_QUEUE_SIZE);
  });

  it('round-trips wrap(unwrap(x)) back to x for every network-backed event', () => {
    for (const eventName of [REQUEST, REQUEST_TIMEOUT, REQUEST_QUEUE_SIZE]) {
      expect(wrap(unwrap(eventName))).toBe(eventName);
    }
  });
});
