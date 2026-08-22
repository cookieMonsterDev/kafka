import { EventEmitter } from 'node:events';
import { KafkaInvariantViolation } from '../../errors';
import type { InstrumentationEventEmitter } from '../../instrumentation/emitter';
import type { Logger } from '../../loggers/index';
import { NETWORK_REQUEST_QUEUE_SIZE } from '../instrumentation-events';
import type { NetworkEventMap } from '../instrumentation-events';
import { SocketRequest } from './socket-request';
import type { RequestEntry } from './socket-request';

const REQUEST_QUEUE_EMPTY = 'requestQueueEmpty';

export interface PushedRequest {
  entry: RequestEntry;
  expectResponse: boolean;
  sendRequest: () => void;
  requestTimeout?: number | null;
}

export interface RequestQueueOptions {
  maxInFlightRequests: number | null;
  requestTimeout: number;
  enforceRequestTimeout: boolean;
  clientId: string;
  broker: string;
  logger: Logger;
  instrumentationEmitter?: InstrumentationEventEmitter<NetworkEventMap> | null;
  isConnected?: () => boolean;
}

/**
 * Owns every in-flight and queued request for one `Connection`: enforces `maxInFlightRequests`,
 * matches correlation ids, and applies client-side throttling from KIP-219. Per-request timeouts
 * are `SocketRequest`'s own `AbortSignal.timeout()`, not tracked here.
 */
export class RequestQueue extends EventEmitter {
  readonly instrumentationEmitter: InstrumentationEventEmitter<NetworkEventMap> | null;
  readonly maxInFlightRequests: number | null;
  readonly requestTimeout: number;
  readonly enforceRequestTimeout: boolean;
  readonly clientId: string;
  readonly broker: string;
  readonly logger: Logger;
  readonly isConnected: () => boolean;

  readonly inflight = new Map<number, SocketRequest>();
  pending: SocketRequest[] = [];

  /**
   * Until when this request queue is throttled and shouldn't send requests: a ms-since-epoch
   * timestamp. A value in the past (or `-1`) means no throttling is active.
   */
  throttledUntil = -1;

  throttleCheckTimeoutId: NodeJS.Timeout | null = null;

  constructor(options: RequestQueueOptions) {
    super();
    this.instrumentationEmitter = options.instrumentationEmitter ?? null;
    this.maxInFlightRequests = options.maxInFlightRequests;
    this.requestTimeout = options.requestTimeout;
    this.enforceRequestTimeout = options.enforceRequestTimeout;
    this.clientId = options.clientId;
    this.broker = options.broker;
    this.logger = options.logger;
    this.isConnected = options.isConnected ?? (() => true);
  }

  #emitRequestQueueEmptyIfIdle(): void {
    if (this.pending.length === 0 && this.inflight.size === 0) {
      this.emit(REQUEST_QUEUE_EMPTY);
    }
  }

  #emitQueueSizeEvent(): void {
    this.instrumentationEmitter?.emit(NETWORK_REQUEST_QUEUE_SIZE, {
      broker: this.broker,
      clientId: this.clientId,
      queueSize: this.pending.length,
    });

    this.#emitRequestQueueEmptyIfIdle();
  }

  maybeThrottle(clientSideThrottleTime: number | null | undefined): void {
    if (clientSideThrottleTime != null && clientSideThrottleTime > 0) {
      this.logger.debug(`Client side throttling in effect for ${clientSideThrottleTime}ms`);
      const minimumThrottledUntil = Date.now() + clientSideThrottleTime;
      this.throttledUntil = Math.max(minimumThrottledUntil, this.throttledUntil);
    }
  }

  createSocketRequest(pushedRequest: PushedRequest): SocketRequest {
    const { correlationId } = pushedRequest.entry;
    const defaultRequestTimeout = this.requestTimeout;
    const customRequestTimeout = pushedRequest.requestTimeout;

    // Some protocol requests have custom request timeouts (e.g. JoinGroup, Fetch, etc). The custom
    // timeouts are influenced by user configuration, which can be lower than the default requestTimeout.
    const requestTimeout = Math.max(defaultRequestTimeout, customRequestTimeout ?? 0);

    const socketRequest = new SocketRequest({
      entry: pushedRequest.entry,
      expectResponse: pushedRequest.expectResponse,
      broker: this.broker,
      clientId: this.clientId,
      instrumentationEmitter: this.instrumentationEmitter,
      requestTimeout,
      enforceRequestTimeout: this.enforceRequestTimeout,
      send: () => {
        if (this.inflight.has(correlationId)) {
          throw new KafkaInvariantViolation('Correlation id already exists');
        }
        this.inflight.set(correlationId, socketRequest);
        pushedRequest.sendRequest();
      },
      timeout: () => {
        this.inflight.delete(correlationId);
        this.checkPendingRequests();
        // Try to emit REQUEST_QUEUE_EMPTY. Otherwise, waitForPendingRequests may get stuck forever.
        this.#emitRequestQueueEmptyIfIdle();
      },
    });

    return socketRequest;
  }

  push(pushedRequest: PushedRequest): void {
    const { correlationId } = pushedRequest.entry;
    const socketRequest = this.createSocketRequest(pushedRequest);

    if (this.canSendSocketRequestImmediately()) {
      this.sendSocketRequest(socketRequest);
      return;
    }

    this.pending.push(socketRequest);
    this.scheduleCheckPendingRequests();

    this.logger.debug(`Request enqueued`, {
      clientId: this.clientId,
      broker: this.broker,
      correlationId,
    });

    this.#emitQueueSizeEvent();
  }

  sendSocketRequest(socketRequest: SocketRequest): void {
    socketRequest.send();

    if (!socketRequest.expectResponse) {
      this.logger.debug(`Request does not expect a response, resolving immediately`, {
        clientId: this.clientId,
        broker: this.broker,
        correlationId: socketRequest.correlationId,
      });

      this.inflight.delete(socketRequest.correlationId);
      socketRequest.completed({ size: 0, payload: null });
    }
  }

  fulfillRequest({ correlationId, payload, size }: { correlationId: number; payload: Buffer; size: number }): void {
    const socketRequest = this.inflight.get(correlationId);
    this.inflight.delete(correlationId);
    this.checkPendingRequests();

    if (socketRequest) {
      socketRequest.completed({ size, payload });
    } else {
      this.logger.warn(`Response without match`, {
        clientId: this.clientId,
        broker: this.broker,
        correlationId,
      });
    }

    this.#emitRequestQueueEmptyIfIdle();
  }

  rejectAll(error: unknown): void {
    const requests = [...this.inflight.values(), ...this.pending];

    for (const socketRequest of requests) {
      socketRequest.rejected(error);
      this.inflight.delete(socketRequest.correlationId);
    }

    this.pending = [];
    this.inflight.clear();
    this.#emitQueueSizeEvent();
  }

  waitForPendingRequests(): Promise<void> {
    return new Promise((resolve) => {
      if (this.pending.length === 0 && this.inflight.size === 0) {
        resolve();
        return;
      }

      this.logger.debug('Waiting for pending requests', {
        clientId: this.clientId,
        broker: this.broker,
        currentInflightRequests: this.inflight.size,
        currentPendingQueueSize: this.pending.length,
      });

      this.once(REQUEST_QUEUE_EMPTY, () => resolve());
    });
  }

  destroy(): void {
    clearTimeout(this.throttleCheckTimeoutId ?? undefined);
    this.throttleCheckTimeoutId = null;
  }

  canSendSocketRequestImmediately(): boolean {
    const shouldEnqueue =
      (this.maxInFlightRequests != null && this.inflight.size >= this.maxInFlightRequests) ||
      this.throttledUntil > Date.now();

    return !shouldEnqueue;
  }

  /**
   * Sends out as many pending requests as possible right now, taking throttling and in-flight
   * limits into account, then schedules another check for whatever remains.
   */
  checkPendingRequests(): void {
    while (this.pending.length > 0 && this.canSendSocketRequestImmediately()) {
      const pendingRequest = this.pending.shift()!; // first in, first out
      this.sendSocketRequest(pendingRequest);

      this.logger.debug(`Consumed pending request`, {
        clientId: this.clientId,
        broker: this.broker,
        correlationId: pendingRequest.correlationId,
        pendingDuration: pendingRequest.pendingDuration,
        currentPendingQueueSize: this.pending.length,
      });

      this.#emitQueueSizeEvent();
    }

    this.scheduleCheckPendingRequests();
  }

  /**
   * Ensures pending requests get checked again when client-side throttling lifts. Fulfilling or
   * timing out an in-flight request already calls `checkPendingRequests`, so there is no poll
   * when the queue is only blocked by `maxInFlightRequests`.
   */
  scheduleCheckPendingRequests(): void {
    if (this.throttleCheckTimeoutId) {
      return;
    }

    const throttleDelay = this.throttledUntil - Date.now();
    if (throttleDelay > 0) {
      this.throttleCheckTimeoutId = setTimeout(() => {
        this.throttleCheckTimeoutId = null;
        this.checkPendingRequests();
      }, throttleDelay);
    }
  }
}
