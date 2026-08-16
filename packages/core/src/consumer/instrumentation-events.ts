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
import type { MemberAssignment, TopicOffsets } from './types';

const consumerType = namespace('consumer');

export const HEARTBEAT = consumerType('heartbeat');
export const COMMIT_OFFSETS = consumerType('commit_offsets');
export const GROUP_JOIN = consumerType('group_join');
export const FETCH = consumerType('fetch');
export const FETCH_START = consumerType('fetch_start');
export const START_BATCH_PROCESS = consumerType('start_batch_process');
export const END_BATCH_PROCESS = consumerType('end_batch_process');
export const CONNECT = consumerType('connect');
export const DISCONNECT = consumerType('disconnect');
export const STOP = consumerType('stop');
export const CRASH = consumerType('crash');
export const REBALANCING = consumerType('rebalancing');
export const RECEIVED_UNSUBSCRIBED_TOPICS = consumerType('received_unsubscribed_topics');
export const REQUEST = consumerType(NETWORK_REQUEST);
export const REQUEST_TIMEOUT = consumerType(NETWORK_REQUEST_TIMEOUT);
export const REQUEST_QUEUE_SIZE = consumerType(NETWORK_REQUEST_QUEUE_SIZE);

export const events = Object.freeze({
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

export type ConsumerEventName = (typeof events)[keyof typeof events];

export interface HeartbeatPayload {
  groupId: string;
  memberId: string;
  groupGenerationId: number;
}

export interface CommitOffsetsPayload {
  groupId: string;
  memberId: string;
  groupGenerationId: number;
  topics: TopicOffsets[];
}

export interface GroupJoinPayload {
  duration: number;
  groupId: string;
  isLeader: boolean;
  leaderId: string | null;
  groupProtocol: string | null;
  memberId: string | null;
  memberAssignment: MemberAssignment;
}

export interface FetchStartPayload {
  nodeId: string;
}

export interface FetchPayload {
  numberOfBatches: number;
  duration: number;
  nodeId: string;
}

export interface BatchProcessPayload {
  topic: string;
  partition: number;
  highWatermark: bigint;
  offsetLag: bigint;
  offsetLagLow: bigint;
  batchSize: number;
  firstOffset: bigint | null;
  lastOffset: bigint;
}

export interface CrashPayload {
  error: Error;
  groupId: string;
  restart: boolean;
}

export interface RebalancingPayload {
  groupId: string;
  memberId: string | null;
}

export interface ReceivedUnsubscribedTopicsPayload {
  groupId: string;
  generationId: number | null;
  memberId: string | null;
  assignedTopics: string[];
  topicsSubscribed: string[];
  topicsNotSubscribed: string[];
}

export interface ConsumerEventMap {
  [HEARTBEAT]: HeartbeatPayload;
  [COMMIT_OFFSETS]: CommitOffsetsPayload;
  [GROUP_JOIN]: GroupJoinPayload;
  [FETCH]: FetchPayload;
  [FETCH_START]: FetchStartPayload;
  [START_BATCH_PROCESS]: BatchProcessPayload;
  [END_BATCH_PROCESS]: BatchProcessPayload & { duration: number };
  [CONNECT]: Record<string, never>;
  [DISCONNECT]: Record<string, never>;
  [STOP]: Record<string, never>;
  [CRASH]: CrashPayload;
  [REBALANCING]: RebalancingPayload;
  [RECEIVED_UNSUBSCRIBED_TOPICS]: ReceivedUnsubscribedTopicsPayload;
  [REQUEST]: NetworkRequestEvent;
  [REQUEST_TIMEOUT]: NetworkRequestTimeoutEvent;
  [REQUEST_QUEUE_SIZE]: NetworkRequestQueueSizeEvent;
}

/**
 * The consumer's public event names are namespaced under `consumer.*`, but requests are actually
 * emitted by the shared network layer under `network.*`. `wrap`/`unwrap` translate between the two
 * so `consumer.on(events.REQUEST, ...)` transparently listens to `network.request`.
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
