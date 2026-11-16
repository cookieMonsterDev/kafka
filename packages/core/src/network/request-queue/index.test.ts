import { afterEach, describe, expect, it, vi } from 'vitest';
import { KafkaInvariantViolation, KafkaRequestTimeoutError } from '../../errors';
import { InstrumentationEventEmitter } from '../../instrumentation/emitter';
import { createLogger, LOG_LEVELS } from '../../loggers/index';
import { sleep } from '../../utils/wait';
import { NETWORK_REQUEST_QUEUE_SIZE, NETWORK_REQUEST_TIMEOUT } from '../instrumentation-events';
import type { NetworkEventMap } from '../instrumentation-events';
import { RequestQueue } from './index';
import type { PushedRequest, RequestQueueOptions } from './index';
import type { RequestEntry } from './socket-request';

const testLogger = createLogger({ level: LOG_LEVELS.NOTHING, logCreator: () => () => {} });

describe('network/request-queue/RequestQueue', () => {
  let correlationId = 0;

  const createEntry = (): RequestEntry => ({
    apiKey: 0,
    apiName: 'Produce',
    apiVersion: 4,
    correlationId: correlationId++,
    resolve: vi.fn(),
    reject: vi.fn(),
  });

  const requestQueues: RequestQueue[] = [];
  const createRequestQueue = (overrides: Partial<RequestQueueOptions> = {}) => {
    const requestQueue = new RequestQueue({
      maxInFlightRequests: 2,
      requestTimeout: 50,
      enforceRequestTimeout: true,
      clientId: 'kafka',
      broker: 'localhost:9092',
      logger: testLogger,
      ...overrides,
    });
    requestQueues.push(requestQueue);
    return requestQueue;
  };

  // A queue left with requests in flight would time them out during a later test, sending its pending
  // requests and arming a timer under that test's fake timers.
  afterEach(() => {
    for (const requestQueue of requestQueues.splice(0)) requestQueue.destroy();
  });

  const createPushedRequest = (overrides: Partial<PushedRequest> = {}): PushedRequest => ({
    sendRequest: vi.fn(),
    entry: createEntry(),
    expectResponse: true,
    ...overrides,
  });

  describe('push', () => {
    it('sends immediately when under the in-flight limit', () => {
      const requestQueue = createRequestQueue();
      const request = createPushedRequest();

      requestQueue.push(request);
      expect(request.sendRequest).toHaveBeenCalledTimes(1);
      expect(requestQueue.inflight.size).toBe(1);
      expect(requestQueue.pending.length).toBe(0);
    });

    it('resolves immediately with no payload when the request does not expect a response', () => {
      const requestQueue = createRequestQueue();
      const request = createPushedRequest({ expectResponse: false });

      requestQueue.push(request);

      expect(request.entry.resolve).toHaveBeenCalledWith(expect.objectContaining({ size: 0, payload: null }));
      expect(requestQueue.inflight.size).toBe(0);
    });

    it('queues requests beyond maxInFlightRequests as pending', () => {
      const requestQueue = createRequestQueue({ maxInFlightRequests: 2 });
      requestQueue.push(createPushedRequest());
      requestQueue.push(createPushedRequest());

      const overflow = createPushedRequest();
      requestQueue.push(overflow);

      expect(requestQueue.inflight.size).toBe(2);
      expect(requestQueue.pending.length).toBe(1);
      expect(overflow.sendRequest).not.toHaveBeenCalled();
    });

    it('does not enforce the in-flight limit when maxInFlightRequests is null', () => {
      const requestQueue = createRequestQueue({ maxInFlightRequests: null });
      requestQueue.push(createPushedRequest());
      requestQueue.push(createPushedRequest());
      requestQueue.push(createPushedRequest());

      expect(requestQueue.inflight.size).toBe(3);
      expect(requestQueue.pending.length).toBe(0);
    });

    it('enforces a numeric maxInFlightRequests cap of 5', () => {
      const requestQueue = createRequestQueue({ maxInFlightRequests: 5 });
      const first = createPushedRequest();
      requestQueue.push(first);
      for (let i = 0; i < 4; i++) {
        requestQueue.push(createPushedRequest());
      }

      const overflow = createPushedRequest();
      requestQueue.push(overflow);

      expect(requestQueue.inflight.size).toBe(5);
      expect(requestQueue.pending.length).toBe(1);
      expect(overflow.sendRequest).not.toHaveBeenCalled();

      requestQueue.fulfillRequest({
        correlationId: first.entry.correlationId,
        payload: Buffer.alloc(0),
        size: 0,
      });

      expect(overflow.sendRequest).toHaveBeenCalledTimes(1);
      expect(requestQueue.inflight.size).toBe(5);
      expect(requestQueue.pending.length).toBe(0);
    });

    it('does not poll pending requests on a timer when blocked only by maxInFlightRequests', async () => {
      const requestQueue = createRequestQueue({ maxInFlightRequests: 1 });
      requestQueue.push(createPushedRequest());
      const pending = createPushedRequest();
      requestQueue.push(pending);

      await sleep(30);

      expect(pending.sendRequest).not.toHaveBeenCalled();
      expect(requestQueue.pending.length).toBe(1);
    });

    it('throws KafkaInvariantViolation on a correlation id collision', () => {
      const requestQueue = createRequestQueue();
      const request = createPushedRequest();
      requestQueue.inflight.set(request.entry.correlationId, {} as never);

      expect(() => requestQueue.push(request)).toThrow(new KafkaInvariantViolation('Correlation id already exists'));
    });

    it('delays sending until client-side throttling lifts', async () => {
      const requestQueue = createRequestQueue();
      let sentAt: number | undefined;
      const request = createPushedRequest({ sendRequest: () => (sentAt = Date.now()) });

      const before = Date.now();
      const clientSideThrottleTime = 200;
      requestQueue.maybeThrottle(clientSideThrottleTime);
      expect(requestQueue.throttledUntil).toBeGreaterThanOrEqual(before + clientSideThrottleTime);

      requestQueue.push(request);
      await sleep(clientSideThrottleTime + 50);

      expect(sentAt).toBeGreaterThanOrEqual(before + clientSideThrottleTime);
    });
  });

  describe('fulfillRequest', () => {
    it('deletes the in-flight request and resolves it', () => {
      const requestQueue = createRequestQueue();
      const request = createPushedRequest();
      requestQueue.push(request);

      const payload = Buffer.from('ok');
      requestQueue.fulfillRequest({ correlationId: request.entry.correlationId, payload, size: 32 });

      expect(requestQueue.inflight.size).toBe(0);
      expect(request.entry.resolve).toHaveBeenCalledWith(expect.objectContaining({ size: 32, payload }));
    });

    it('sends the earliest pending request once an in-flight slot frees up', () => {
      const requestQueue = createRequestQueue({ maxInFlightRequests: 1 });
      const first = createPushedRequest();
      requestQueue.push(first);

      const pending = createPushedRequest();
      requestQueue.push(pending);
      expect(requestQueue.pending.length).toBe(1);

      requestQueue.fulfillRequest({ correlationId: first.entry.correlationId, payload: Buffer.alloc(0), size: 0 });

      expect(pending.sendRequest).toHaveBeenCalledTimes(1);
      expect(requestQueue.pending.length).toBe(0);
      expect(requestQueue.inflight.size).toBe(1);
    });

    it('logs a warning and is a no-op for an unmatched correlation id', () => {
      const requestQueue = createRequestQueue();
      expect(() =>
        requestQueue.fulfillRequest({ correlationId: 999, payload: Buffer.alloc(0), size: 0 }),
      ).not.toThrow();
    });
  });

  describe('rejectAll', () => {
    it('rejects every in-flight and pending request', () => {
      const requestQueue = createRequestQueue({ maxInFlightRequests: 1 });
      const inflightRequest = createPushedRequest();
      requestQueue.push(inflightRequest);

      const pendingRequest = createPushedRequest();
      requestQueue.push(pendingRequest);

      expect(requestQueue.inflight.size).toBe(1);
      expect(requestQueue.pending.length).toBe(1);

      const error = new Error('Broker closed the connection');
      requestQueue.rejectAll(error);

      expect(requestQueue.inflight.size).toBe(0);
      expect(requestQueue.pending.length).toBe(0);
      expect(inflightRequest.entry.reject).toHaveBeenCalledWith(error);
      expect(pendingRequest.entry.reject).toHaveBeenCalledWith(error);
    });
  });

  describe('waitForPendingRequests', () => {
    it('resolves immediately when nothing is in flight or pending', async () => {
      const requestQueue = createRequestQueue();
      await expect(requestQueue.waitForPendingRequests()).resolves.toBeUndefined();
    });

    it('blocks until a pending request is fulfilled', async () => {
      const requestQueue = createRequestQueue({ maxInFlightRequests: 1 });
      requestQueue.push(createPushedRequest());
      const pending = createPushedRequest();
      requestQueue.push(pending);

      const waiting = requestQueue.waitForPendingRequests();
      let resolved = false;
      void waiting.then(() => (resolved = true));

      await sleep(10);
      expect(resolved).toBe(false);

      for (const correlationId of [...requestQueue.inflight.keys()]) {
        requestQueue.fulfillRequest({ correlationId, payload: Buffer.alloc(0), size: 0 });
      }
      requestQueue.fulfillRequest({ correlationId: pending.entry.correlationId, payload: Buffer.alloc(0), size: 0 });

      await waiting;
      expect(resolved).toBe(true);
    });

    it('gives up after the optional timeout even if requests are still in flight', async () => {
      const requestQueue = createRequestQueue({ enforceRequestTimeout: false });
      requestQueue.push(createPushedRequest());

      await requestQueue.waitForPendingRequests(10);
      expect(requestQueue.inflight.size).toBe(1);
      expect(requestQueue.listenerCount('requestQueueEmpty')).toBe(0);
    });

    it('unblocks once an in-flight request times out on its own', async () => {
      const requestQueue = createRequestQueue({ requestTimeout: 20, enforceRequestTimeout: true });
      requestQueue.push(createPushedRequest());

      expect(requestQueue.inflight.size).toBe(1);
      await requestQueue.waitForPendingRequests();
      expect(requestQueue.inflight.size).toBe(0);
    });
  });

  describe('request timeout', () => {
    afterEach(() => {
      // Restore spies before the fake timers, or restoring a spy would reinstall a fake setTimeout.
      vi.restoreAllMocks();
      vi.useRealTimers();
    });

    const fulfill = (requestQueue: RequestQueue, request: PushedRequest): void =>
      requestQueue.fulfillRequest({ correlationId: request.entry.correlationId, payload: Buffer.alloc(0), size: 0 });

    it('rejects an in-flight request with its timing fields once requestTimeout elapses', async () => {
      vi.useFakeTimers();
      const requestQueue = createRequestQueue({ requestTimeout: 200 });
      const request = createPushedRequest();
      requestQueue.push(request);
      const socketRequest = requestQueue.inflight.get(request.entry.correlationId)!;

      await vi.advanceTimersByTimeAsync(199);
      expect(request.entry.reject).not.toHaveBeenCalled();

      await vi.advanceTimersByTimeAsync(1);
      const error = vi.mocked(request.entry.reject).mock.calls[0]?.[0] as KafkaRequestTimeoutError;
      expect(error).toBeInstanceOf(KafkaRequestTimeoutError);
      expect(error.message).toBe('Request Produce(key: 0, version: 4) timed out');
      expect(error).toMatchObject({
        broker: 'localhost:9092',
        correlationId: request.entry.correlationId,
        createdAt: socketRequest.createdAt,
        sentAt: socketRequest.sentAt,
        pendingDuration: socketRequest.pendingDuration,
      });
      expect(requestQueue.inflight.size).toBe(0);
      expect(vi.getTimerCount()).toBe(0);
    });

    it('arms one timer for the connection, not one per request', () => {
      vi.useFakeTimers();
      const setTimeoutSpy = vi.spyOn(globalThis, 'setTimeout');
      const requestQueue = createRequestQueue({ maxInFlightRequests: null });

      const concurrent = [createPushedRequest(), createPushedRequest()];
      for (const request of concurrent) requestQueue.push(request);
      for (const request of concurrent) fulfill(requestQueue, request);
      for (let index = 0; index < 3; index++) {
        const request = createPushedRequest();
        requestQueue.push(request);
        fulfill(requestQueue, request);
      }

      expect(setTimeoutSpy).toHaveBeenCalledOnce();
      expect(vi.getTimerCount()).toBe(1);
    });

    it('times out the next request at its own deadline once the oldest completes', async () => {
      vi.useFakeTimers();
      const requestQueue = createRequestQueue({ requestTimeout: 200 });
      const oldest = createPushedRequest();
      requestQueue.push(oldest);
      await vi.advanceTimersByTimeAsync(100);
      const next = createPushedRequest();
      requestQueue.push(next);
      await vi.advanceTimersByTimeAsync(50);
      fulfill(requestQueue, oldest);

      // The timer still fires at the oldest's deadline, then re-arms for the next one's.
      await vi.advanceTimersByTimeAsync(149);
      expect(next.entry.reject).not.toHaveBeenCalled();

      await vi.advanceTimersByTimeAsync(1);
      expect(next.entry.reject).toHaveBeenCalledWith(expect.any(KafkaRequestTimeoutError));
      expect(oldest.entry.reject).not.toHaveBeenCalled();
    });

    it('times out each request at its own deadline when a longer custom requestTimeout is sent first', async () => {
      vi.useFakeTimers();
      const requestQueue = createRequestQueue({ requestTimeout: 200 });
      const custom = createPushedRequest({ requestTimeout: 1000 });
      const regular = createPushedRequest();
      requestQueue.push(custom);
      requestQueue.push(regular);

      await vi.advanceTimersByTimeAsync(200);
      expect(regular.entry.reject).toHaveBeenCalledWith(expect.any(KafkaRequestTimeoutError));
      expect(custom.entry.reject).not.toHaveBeenCalled();

      await vi.advanceTimersByTimeAsync(799);
      expect(custom.entry.reject).not.toHaveBeenCalled();
      await vi.advanceTimersByTimeAsync(1);
      expect(custom.entry.reject).toHaveBeenCalledWith(expect.any(KafkaRequestTimeoutError));
    });

    it('is not moved by a wall-clock jump', async () => {
      vi.useFakeTimers();
      const requestQueue = createRequestQueue({ requestTimeout: 200 });
      const request = createPushedRequest();
      requestQueue.push(request);

      vi.setSystemTime(Date.now() + 60_000);
      await vi.advanceTimersByTimeAsync(199);
      expect(request.entry.reject).not.toHaveBeenCalled();
      await vi.advanceTimersByTimeAsync(1);
      expect(request.entry.reject).toHaveBeenCalledWith(expect.any(KafkaRequestTimeoutError));
    });

    it('waits out a requestTimeout beyond the longest timer delay without firing every millisecond', async () => {
      vi.useFakeTimers();
      const setTimeoutSpy = vi.spyOn(globalThis, 'setTimeout');
      const requestQueue = createRequestQueue({ requestTimeout: 2 ** 31 + 1000 });
      const request = createPushedRequest();
      requestQueue.push(request);

      await vi.advanceTimersByTimeAsync(100);
      expect(setTimeoutSpy).toHaveBeenCalledOnce();
      expect(request.entry.reject).not.toHaveBeenCalled();
    });

    it('does not time out a request that already completed', async () => {
      vi.useFakeTimers();
      const requestQueue = createRequestQueue({ requestTimeout: 200 });
      const request = createPushedRequest();
      requestQueue.push(request);
      fulfill(requestQueue, request);

      await vi.advanceTimersByTimeAsync(1000);
      expect(request.entry.reject).not.toHaveBeenCalled();
      expect(vi.getTimerCount()).toBe(0);
    });

    it('arms no timer when enforceRequestTimeout is false', async () => {
      vi.useFakeTimers();
      const requestQueue = createRequestQueue({ requestTimeout: 200, enforceRequestTimeout: false });
      const request = createPushedRequest();
      requestQueue.push(request);
      expect(vi.getTimerCount()).toBe(0);

      await vi.advanceTimersByTimeAsync(1000);
      expect(request.entry.reject).not.toHaveBeenCalled();
    });

    it('arms no timer for a request that expects no response', () => {
      vi.useFakeTimers();
      const requestQueue = createRequestQueue();
      requestQueue.push(createPushedRequest({ expectResponse: false }));
      expect(vi.getTimerCount()).toBe(0);
    });

    it("keeps the timer unref'd and clears it on destroy", () => {
      vi.useFakeTimers();
      const setTimeoutSpy = vi.spyOn(globalThis, 'setTimeout');
      const requestQueue = createRequestQueue();
      requestQueue.push(createPushedRequest());

      const timer = setTimeoutSpy.mock.results[0]?.value as NodeJS.Timeout;
      expect(timer.hasRef()).toBe(false);
      requestQueue.destroy();
      expect(vi.getTimerCount()).toBe(0);
    });
  });

  describe('instrumentation events', () => {
    it('does not emit when the queue size does not change', () => {
      const emitter = new InstrumentationEventEmitter<NetworkEventMap>();
      const eventCalled = vi.fn();
      emitter.addListener(NETWORK_REQUEST_QUEUE_SIZE, eventCalled);

      const requestQueue = createRequestQueue({ instrumentationEmitter: emitter });
      requestQueue.push(createPushedRequest());

      expect(eventCalled).not.toHaveBeenCalled();
    });

    it('emits NETWORK_REQUEST_QUEUE_SIZE when a request is enqueued as pending', () => {
      const emitter = new InstrumentationEventEmitter<NetworkEventMap>();
      const eventCalled = vi.fn();
      emitter.addListener(NETWORK_REQUEST_QUEUE_SIZE, eventCalled);

      const requestQueue = createRequestQueue({ instrumentationEmitter: emitter, maxInFlightRequests: 1 });
      requestQueue.push(createPushedRequest());
      requestQueue.push(createPushedRequest());

      expect(eventCalled).toHaveBeenCalledWith(
        expect.objectContaining({
          type: NETWORK_REQUEST_QUEUE_SIZE,
          payload: { broker: 'localhost:9092', clientId: 'kafka', queueSize: 1 },
        }),
      );
    });

    it('emits NETWORK_REQUEST_QUEUE_SIZE when the requests are rejected', () => {
      const emitter = new InstrumentationEventEmitter<NetworkEventMap>();
      const eventCalled = vi.fn();
      emitter.addListener(NETWORK_REQUEST_QUEUE_SIZE, eventCalled);

      const requestQueue = createRequestQueue({ instrumentationEmitter: emitter });
      requestQueue.rejectAll(new Error('closed'));

      expect(eventCalled).toHaveBeenCalledWith(
        expect.objectContaining({
          type: NETWORK_REQUEST_QUEUE_SIZE,
          payload: { broker: 'localhost:9092', clientId: 'kafka', queueSize: 0 },
        }),
      );
    });

    it('emits NETWORK_REQUEST_TIMEOUT when an in-flight request times out', async () => {
      const emitter = new InstrumentationEventEmitter<NetworkEventMap>();
      const eventCalled = vi.fn();
      emitter.addListener(NETWORK_REQUEST_TIMEOUT, eventCalled);

      const requestQueue = createRequestQueue({ instrumentationEmitter: emitter, requestTimeout: 20 });
      const request = createPushedRequest();
      requestQueue.push(request);

      await sleep(60);

      expect(eventCalled).toHaveBeenCalledWith(
        expect.objectContaining({
          type: NETWORK_REQUEST_TIMEOUT,
          payload: expect.objectContaining({
            apiKey: request.entry.apiKey,
            apiName: request.entry.apiName,
            apiVersion: request.entry.apiVersion,
            broker: 'localhost:9092',
            clientId: 'kafka',
          }),
        }),
      );
    });
  });

  describe('destroy', () => {
    it('clears the scheduled throttle check without throwing', () => {
      const requestQueue = createRequestQueue();
      requestQueue.maybeThrottle(1000);
      requestQueue.push(createPushedRequest());
      expect(() => requestQueue.destroy()).not.toThrow();
    });
  });
});
