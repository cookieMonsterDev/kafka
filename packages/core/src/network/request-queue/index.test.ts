import { describe, expect, it, vi } from 'vitest';
import { KafkaJSInvariantViolation } from '../../errors.js';
import { InstrumentationEventEmitter } from '../../instrumentation/emitter.js';
import { createLogger, LOG_LEVELS } from '../../loggers/index.js';
import { sleep } from '../../utils/wait.js';
import { NETWORK_REQUEST_QUEUE_SIZE, NETWORK_REQUEST_TIMEOUT } from '../instrumentation-events.js';
import type { NetworkEventMap } from '../instrumentation-events.js';
import { RequestQueue } from './index.js';
import type { PushedRequest, RequestQueueOptions } from './index.js';
import type { RequestEntry } from './socket-request.js';

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

  const createRequestQueue = (overrides: Partial<RequestQueueOptions> = {}) =>
    new RequestQueue({
      maxInFlightRequests: 2,
      requestTimeout: 50,
      enforceRequestTimeout: true,
      clientId: 'kafka',
      broker: 'localhost:9092',
      logger: testLogger,
      ...overrides,
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

    it('throws KafkaJSInvariantViolation on a correlation id collision', () => {
      const requestQueue = createRequestQueue();
      const request = createPushedRequest();
      requestQueue.inflight.set(request.entry.correlationId, {} as never);

      expect(() => requestQueue.push(request)).toThrow(new KafkaJSInvariantViolation('Correlation id already exists'));
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

    it('unblocks once an in-flight request times out on its own', async () => {
      const requestQueue = createRequestQueue({ requestTimeout: 20, enforceRequestTimeout: true });
      requestQueue.push(createPushedRequest());

      expect(requestQueue.inflight.size).toBe(1);
      await requestQueue.waitForPendingRequests();
      expect(requestQueue.inflight.size).toBe(0);
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
