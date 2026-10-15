import type { InstrumentationEventEmitter } from '../instrumentation/emitter';
import { namespace } from '../instrumentation/event-type';
import type { Logger } from '../loggers/index';

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

/**
 * Emits a network event, logging a throwing listener instead of propagating it: these events fire
 * from inside the socket `data` handler and request queue, where a user listener's error would
 * otherwise escape as an uncaught exception and leave the connection mid-frame.
 */
export function emitNetworkEvent<EventName extends keyof NetworkEventMap>(
  emitter: InstrumentationEventEmitter<NetworkEventMap> | null | undefined,
  logger: Logger,
  eventName: EventName,
  payload: NetworkEventMap[EventName],
): void {
  try {
    emitter?.emit(eventName, payload);
  } catch (e) {
    const error = e as Error;
    logger.error('Instrumentation listener failed', { eventName, error: error.message, stack: error.stack });
  }
}
