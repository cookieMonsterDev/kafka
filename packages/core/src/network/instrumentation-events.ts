import { namespace } from '../instrumentation/event-type';

const eventType = namespace('network');

export const NETWORK_REQUEST = eventType('request');
export const NETWORK_REQUEST_TIMEOUT = eventType('request_timeout');
export const NETWORK_REQUEST_QUEUE_SIZE = eventType('request_queue_size');

export interface NetworkRequestEvent {
  broker: string;
  clientId: string;
  correlationId: number;
  size: number;
  createdAt: number;
  sentAt: number;
  pendingDuration: number;
  duration: number;
  apiName: string;
  apiKey: number;
  apiVersion: number;
}

export interface NetworkRequestTimeoutEvent {
  broker: string;
  clientId: string;
  correlationId: number;
  createdAt: number;
  sentAt: number | null;
  pendingDuration: number | null;
  apiName: string;
  apiKey: number;
  apiVersion: number;
}

export interface NetworkRequestQueueSizeEvent {
  broker: string;
  clientId: string;
  queueSize: number;
}

export interface NetworkEventMap {
  [NETWORK_REQUEST]: NetworkRequestEvent;
  [NETWORK_REQUEST_TIMEOUT]: NetworkRequestTimeoutEvent;
  [NETWORK_REQUEST_QUEUE_SIZE]: NetworkRequestQueueSizeEvent;
}
