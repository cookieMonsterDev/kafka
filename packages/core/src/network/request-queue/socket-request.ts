import { KafkaNonRetriableError, KafkaRequestTimeoutError } from '../../errors';
import type { InstrumentationEventEmitter } from '../../instrumentation/emitter';
import { NETWORK_REQUEST, NETWORK_REQUEST_TIMEOUT } from '../instrumentation-events';
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
}

/**
 * Tracks one in-flight request (pending → sent → completed/rejected).
 * Timeout is a single `AbortSignal.timeout()` armed at `send()` and cleared on settle.
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
  #timeoutSignal: AbortSignal | undefined;
  #onTimeout = (): void => this.timeoutRequest();
  #emitEvent: <EventName extends keyof NetworkEventMap>(
    eventName: EventName,
    payload: NetworkEventMap[EventName],
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

    const emitter = options.instrumentationEmitter ?? null;
    this.#emitEvent = (eventName, payload) => emitter?.emit(eventName, payload);
  }

  send(): void {
    this.#throwIfInvalidState({ accepted: ['pending'], next: 'sent' });

    this.#sendRequest();
    this.sentAt = Date.now();
    this.pendingDuration = this.sentAt - this.createdAt;
    this.#state = 'sent';

    if (this.#enforceRequestTimeout) {
      this.#timeoutSignal = AbortSignal.timeout(this.requestTimeout);
      this.#timeoutSignal.addEventListener('abort', this.#onTimeout, { once: true });
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

    this.#emitEvent(NETWORK_REQUEST, {
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
    });
  }

  rejected(error: unknown): void {
    this.#throwIfInvalidState({ accepted: ['pending', 'sent'], next: 'rejected' });
    this.#disarmTimeout();

    this.#state = 'rejected';
    this.duration = this.sentAt == null ? null : Date.now() - this.sentAt;
    this.entry.reject(error);
  }

  #disarmTimeout(): void {
    this.#timeoutSignal?.removeEventListener('abort', this.#onTimeout);
    this.#timeoutSignal = undefined;
  }

  #throwIfInvalidState({ accepted, next }: { accepted: RequestState[]; next: RequestState }): void {
    if (accepted.includes(this.#state)) return;
    throw new KafkaNonRetriableError(`Invalid state, can't transition from ${this.#state} to ${next}`);
  }
}
