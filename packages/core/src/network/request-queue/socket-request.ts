import { KafkaNonRetriableError, KafkaRequestTimeoutError } from '../../errors';
import type { InstrumentationEventEmitter, InstrumentationPayload } from '../../instrumentation/emitter';
import type { Logger } from '../../loggers/index';
import { emitNetworkEvent, NETWORK_REQUEST, NETWORK_REQUEST_TIMEOUT } from '../instrumentation-events';
import type { NetworkEventMap } from '../instrumentation-events';

export interface RequestEntry {
  apiKey: number;
  apiName: string;
  apiVersion: number;
  correlationId: number;
  resolve: (value: { correlationId: number; entry: RequestEntry; size: number; payload: Buffer | null }) => void;
  reject: (error: unknown) => void;
}

type RequestState = 'pending' | 'sent' | 'completed' | 'rejected';

export interface SocketRequestOptions {
  requestTimeout: number;
  /** When false, the request never times out on its own — mirrors the `Connection`-level config flag. */
  enforceRequestTimeout: boolean;
  broker: string;
  clientId: string;
  entry: RequestEntry;
  expectResponse: boolean;
  send: () => void;
  timeout: () => void;
  instrumentationEmitter?: InstrumentationEventEmitter<NetworkEventMap> | null;
  /** Receives errors thrown by instrumentation listeners, which are never propagated. */
  logger: Logger;
}

/**
 * Tracks one in-flight request (pending → sent → completed/rejected).
 * Timeout is a single unref'd timer armed at `send()` and cleared on settle. Not
 * `AbortSignal.timeout()`: its timer can't be cancelled, so every settled request would keep a
 * live timer (plus GC bookkeeping for the signal) until `requestTimeout` elapsed.
 */
export class SocketRequest {
  readonly createdAt = Date.now();
  readonly requestTimeout: number;
  readonly broker: string;
  readonly clientId: string;
  readonly entry: RequestEntry;
  readonly correlationId: number;
  readonly expectResponse: boolean;

  sentAt: number | null = null;
  duration: number | null = null;
  pendingDuration: number | null = null;

  #state: RequestState = 'pending';
  #enforceRequestTimeout: boolean;
  #sendRequest: () => void;
  #timeoutHandler: () => void;
  #timeoutTimer: ReturnType<typeof setTimeout> | undefined;
  #onTimeout = (): void => {
    this.#timeoutTimer = undefined;
    this.timeoutRequest();
  };
  #emitEvent: <EventName extends keyof NetworkEventMap>(
    eventName: EventName,
    payload: InstrumentationPayload<NetworkEventMap[EventName]>,
  ) => void;

  constructor(options: SocketRequestOptions) {
    this.requestTimeout = options.requestTimeout;
    this.#enforceRequestTimeout = options.enforceRequestTimeout;
    this.broker = options.broker;
    this.clientId = options.clientId;
    this.entry = options.entry;
    this.correlationId = options.entry.correlationId;
    this.expectResponse = options.expectResponse;
    this.#sendRequest = options.send;
    this.#timeoutHandler = options.timeout;

    const { instrumentationEmitter, logger } = options;
    this.#emitEvent = (eventName, payload) => emitNetworkEvent(instrumentationEmitter, logger, eventName, payload);
  }

  send(): void {
    this.#throwIfInvalidState({ accepted: ['pending'], next: 'sent' });

    this.#sendRequest();
    this.sentAt = Date.now();
    this.pendingDuration = this.sentAt - this.createdAt;
    this.#state = 'sent';

    if (this.#enforceRequestTimeout) {
      // Unref'd like `AbortSignal.timeout()`, so a pending request alone doesn't keep the process alive.
      this.#timeoutTimer = setTimeout(this.#onTimeout, this.requestTimeout);
      this.#timeoutTimer.unref();
    }
  }

  timeoutRequest(): void {
    const { apiName, apiKey, apiVersion } = this.entry;
    const requestInfo = `${apiName}(key: ${apiKey}, version: ${apiVersion})`;
    const eventData = {
      broker: this.broker,
      clientId: this.clientId,
      correlationId: this.correlationId,
      createdAt: this.createdAt,
      sentAt: this.sentAt,
      pendingDuration: this.pendingDuration,
    };

    this.#timeoutHandler();
    this.rejected(
      new KafkaRequestTimeoutError(`Request ${requestInfo} timed out`, {
        ...eventData,
        sentAt: eventData.sentAt ?? undefined,
        pendingDuration: eventData.pendingDuration ?? undefined,
      }),
    );
    this.#emitEvent(NETWORK_REQUEST_TIMEOUT, { ...eventData, apiName, apiKey, apiVersion });
  }

  completed({ size, payload }: { size: number; payload: Buffer | null }): void {
    this.#throwIfInvalidState({ accepted: ['sent'], next: 'completed' });
    this.#disarmTimeout();

    const { entry, correlationId, broker, clientId, createdAt, sentAt, pendingDuration } = this;
    const duration = Date.now() - sentAt!;

    this.#state = 'completed';
    this.duration = duration;
    entry.resolve({ correlationId, entry, size, payload });

    this.#emitEvent(NETWORK_REQUEST, () => ({
      broker,
      clientId,
      correlationId,
      size,
      createdAt,
      sentAt: sentAt!,
      pendingDuration: pendingDuration!,
      duration,
      apiName: entry.apiName,
      apiKey: entry.apiKey,
      apiVersion: entry.apiVersion,
    }));
  }

  rejected(error: unknown): void {
    this.#throwIfInvalidState({ accepted: ['pending', 'sent'], next: 'rejected' });
    this.#disarmTimeout();

    this.#state = 'rejected';
    this.duration = this.sentAt == null ? null : Date.now() - this.sentAt;
    this.entry.reject(error);
  }

  #disarmTimeout(): void {
    if (this.#timeoutTimer !== undefined) {
      clearTimeout(this.#timeoutTimer);
      this.#timeoutTimer = undefined;
    }
  }

  #throwIfInvalidState({ accepted, next }: { accepted: RequestState[]; next: RequestState }): void {
    if (accepted.includes(this.#state)) return;
    throw new KafkaNonRetriableError(`Invalid state, can't transition from ${this.#state} to ${next}`);
  }
}
