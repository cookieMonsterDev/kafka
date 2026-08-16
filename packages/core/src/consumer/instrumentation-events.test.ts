import { describe, expect, it } from 'vitest';
import {
  COMMIT_OFFSETS,
  CONNECT,
  CRASH,
  DISCONNECT,
  END_BATCH_PROCESS,
  events,
  FETCH,
  FETCH_START,
  GROUP_JOIN,
  HEARTBEAT,
  REBALANCING,
  RECEIVED_UNSUBSCRIBED_TOPICS,
  REQUEST,
  REQUEST_QUEUE_SIZE,
  REQUEST_TIMEOUT,
  START_BATCH_PROCESS,
  STOP,
  unwrap,
  wrap,
} from './instrumentation-events.js';

describe('consumer/instrumentation-events', () => {
  it('namespaces every event under consumer.*', () => {
    expect(events).toEqual({
      HEARTBEAT,
      COMMIT_OFFSETS,
      GROUP_JOIN,
      FETCH,
      FETCH_START,
      START_BATCH_PROCESS,
      END_BATCH_PROCESS,
      CONNECT,
      DISCONNECT,
      STOP,
      CRASH,
      REBALANCING,
      RECEIVED_UNSUBSCRIBED_TOPICS,
      REQUEST,
      REQUEST_TIMEOUT,
      REQUEST_QUEUE_SIZE,
    });
    expect(HEARTBEAT).toBe('consumer.heartbeat');
    expect(REQUEST).toBe('consumer.network.request');
  });

  it('unwraps a public consumer event name to the underlying network event name', () => {
    expect(unwrap(REQUEST)).toBe('network.request');
    expect(unwrap(REQUEST_TIMEOUT)).toBe('network.request_timeout');
    expect(unwrap(REQUEST_QUEUE_SIZE)).toBe('network.request_queue_size');
  });

  it('leaves an already-unwrapped or unrelated event name untouched', () => {
    expect(unwrap(events.CONNECT)).toBe(events.CONNECT);
    expect(unwrap('something.else')).toBe('something.else');
  });

  it('wraps an underlying network event name back to its public consumer event name', () => {
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
