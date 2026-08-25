import { namespace } from '../instrumentation/event-type';
import {
  NETWORK_REQUEST,
  NETWORK_REQUEST_QUEUE_SIZE,
  NETWORK_REQUEST_TIMEOUT,
  type NetworkRequestEvent,
  type NetworkRequestQueueSizeEvent,
  type NetworkRequestTimeoutEvent,
} from '../network/instrumentation-events';
import { swapObject } from '../utils/swap-object';
import type { ShareAcknowledgeType } from './acknowledge-types';

const shareConsumerType = namespace('share_consumer');

export const FETCH_START = shareConsumerType('fetch_start');
export const FETCH = shareConsumerType('fetch');
export const ACKNOWLEDGE = shareConsumerType('acknowledge');
export const REQUEST = shareConsumerType(NETWORK_REQUEST);
export const REQUEST_TIMEOUT = shareConsumerType(NETWORK_REQUEST_TIMEOUT);
export const REQUEST_QUEUE_SIZE = shareConsumerType(NETWORK_REQUEST_QUEUE_SIZE);

export const events = Object.freeze({
  FETCH_START,
  FETCH,
  ACKNOWLEDGE,
  REQUEST,
  REQUEST_TIMEOUT,
  REQUEST_QUEUE_SIZE,
});

export type ShareConsumerEventName = (typeof events)[keyof typeof events];

export interface ShareFetchStartPayload {
  nodeId: string;
}

export interface ShareFetchPayload {
  numberOfBatches: number;
  duration: number;
  nodeId: string;
}

/** One acknowledged acquired range within a partition (KIP-932 ShareFetch/ShareAcknowledge). */
export interface ShareAcknowledgeTopicPartitionPayload {
  partition: number;
  firstOffset: bigint;
  lastOffset: bigint;
  acknowledgeType: ShareAcknowledgeType;
}

export interface ShareAcknowledgeTopicPayload {
  topic: string;
  partitions: ShareAcknowledgeTopicPartitionPayload[];
}

/**
 * Emitted whenever acquired records are acknowledged - piggybacked on the next ShareFetch for the
 * node, or via the explicit ShareAcknowledge sent when a share session closes.
 */
export interface ShareAcknowledgePayload {
  groupId: string;
  memberId: string;
  nodeId: string;
  topics: ShareAcknowledgeTopicPayload[];
}

export interface ShareConsumerEventMap {
  [FETCH_START]: ShareFetchStartPayload;
  [FETCH]: ShareFetchPayload;
  [ACKNOWLEDGE]: ShareAcknowledgePayload;
  [REQUEST]: NetworkRequestEvent;
  [REQUEST_TIMEOUT]: NetworkRequestTimeoutEvent;
  [REQUEST_QUEUE_SIZE]: NetworkRequestQueueSizeEvent;
}

/**
 * The share consumer's public event names are namespaced under `share_consumer.*`, but requests
 * are actually emitted by the shared network layer under `network.*`. `wrap`/`unwrap` translate
 * between the two so `shareConsumer.on(events.REQUEST, ...)` transparently listens to
 * `network.request`.
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
