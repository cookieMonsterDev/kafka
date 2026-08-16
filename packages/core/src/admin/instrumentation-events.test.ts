import { describe, expect, it } from 'vitest';
import { events, REQUEST, REQUEST_QUEUE_SIZE, REQUEST_TIMEOUT, unwrap, wrap } from './instrumentation-events';
import {
  NETWORK_REQUEST,
  NETWORK_REQUEST_QUEUE_SIZE,
  NETWORK_REQUEST_TIMEOUT,
} from '../network/instrumentation-events';

describe('admin instrumentation events', () => {
  it('exposes namespaced public event names', () => {
    expect(events.CONNECT).toBe('admin.connect');
    expect(events.DISCONNECT).toBe('admin.disconnect');
    expect(events.REQUEST).toBe('admin.network.request');
  });

  it('unwraps public network event names to the network layer names', () => {
    expect(unwrap(REQUEST)).toBe(NETWORK_REQUEST);
    expect(unwrap(REQUEST_TIMEOUT)).toBe(NETWORK_REQUEST_TIMEOUT);
    expect(unwrap(REQUEST_QUEUE_SIZE)).toBe(NETWORK_REQUEST_QUEUE_SIZE);
    expect(unwrap(events.CONNECT)).toBe(events.CONNECT);
  });

  it('wraps network layer names back to the public admin names', () => {
    expect(wrap(NETWORK_REQUEST)).toBe(REQUEST);
    expect(wrap(NETWORK_REQUEST_TIMEOUT)).toBe(REQUEST_TIMEOUT);
    expect(wrap(NETWORK_REQUEST_QUEUE_SIZE)).toBe(REQUEST_QUEUE_SIZE);
    expect(wrap(events.CONNECT)).toBe(events.CONNECT);
  });
});
