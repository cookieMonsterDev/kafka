import { namespace } from '../instrumentation/event-type.js';
import {
  NETWORK_REQUEST,
  NETWORK_REQUEST_QUEUE_SIZE,
  NETWORK_REQUEST_TIMEOUT,
  type NetworkRequestEvent,
  type NetworkRequestQueueSizeEvent,
  type NetworkRequestTimeoutEvent,
} from '../network/instrumentation-events.js';
import { swapObject } from '../utils/swap-object.js';

const adminType = namespace('admin');

export const CONNECT = adminType('connect');
export const DISCONNECT = adminType('disconnect');
export const REQUEST = adminType(NETWORK_REQUEST);
export const REQUEST_TIMEOUT = adminType(NETWORK_REQUEST_TIMEOUT);
export const REQUEST_QUEUE_SIZE = adminType(NETWORK_REQUEST_QUEUE_SIZE);

export const events = Object.freeze({
  CONNECT,
  DISCONNECT,
  REQUEST,
  REQUEST_TIMEOUT,
  REQUEST_QUEUE_SIZE,
});

export type AdminEventName = (typeof events)[keyof typeof events];

export interface AdminEventMap {
  [CONNECT]: Record<string, never>;
  [DISCONNECT]: Record<string, never>;
  [REQUEST]: NetworkRequestEvent;
  [REQUEST_TIMEOUT]: NetworkRequestTimeoutEvent;
  [REQUEST_QUEUE_SIZE]: NetworkRequestQueueSizeEvent;
}

/**
 * The admin client's public event names are namespaced under `admin.*`, but requests are actually
 * emitted by the shared network layer under `network.*` (the same emitter instance is threaded
 * down through `Cluster` -> `ConnectionPool` -> `Connection`). `wrap`/`unwrap` translate between
 * the two so `admin.on(events.REQUEST, ...)` transparently listens to `network.request`.
 */
const wrappedEvents: Readonly<Record<string, string>> = Object.freeze({
  [REQUEST]: NETWORK_REQUEST,
  [REQUEST_TIMEOUT]: NETWORK_REQUEST_TIMEOUT,
  [REQUEST_QUEUE_SIZE]: NETWORK_REQUEST_QUEUE_SIZE,
});

const reversedWrappedEvents = swapObject(wrappedEvents);

export function unwrap(eventName: string): string {
  return wrappedEvents[eventName] ?? eventName;
}

export function wrap(eventName: string): string {
  return reversedWrappedEvents[eventName] ?? eventName;
}
