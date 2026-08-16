import { describe, expect, it, vi } from 'vitest';
import { KafkaJSNonRetriableError, KafkaJSRequestTimeoutError } from '../../errors.js';
import { InstrumentationEventEmitter } from '../../instrumentation/emitter.js';
import { NETWORK_REQUEST, NETWORK_REQUEST_TIMEOUT } from '../instrumentation-events.js';
import type { NetworkEventMap } from '../instrumentation-events.js';
import { SocketRequest } from './socket-request.js';
import type { RequestEntry, SocketRequestOptions } from './socket-request.js';

describe('network/request-queue/SocketRequest', () => {
  let correlationId = 0;
  const requestTimeout = 50;
  const size = 32;
  const payload = Buffer.from('ok');

  const createEntry = (): RequestEntry => ({
    apiKey: 0,
    apiVersion: 4,
    apiName: 'Produce',
    correlationId: correlationId++,
    resolve: vi.fn(),
    reject: vi.fn(),
  });

  const createSocketRequest = (overrides: Partial<SocketRequestOptions> = {}) =>
    new SocketRequest({
      requestTimeout,
      enforceRequestTimeout: true,
      broker: 'localhost:9092',
      clientId: 'kafka',
      expectResponse: true,
      entry: createEntry(),
      send: vi.fn(),
      timeout: vi.fn(),
      ...overrides,
    });

  describe('send', () => {
    it('sends the request using the provided function', () => {
      const sendRequest = vi.fn();
      const request = createSocketRequest({ send: sendRequest });

      expect(request.sentAt).toBeNull();
      expect(request.pendingDuration).toBeNull();

      request.send();

      expect(sendRequest).toHaveBeenCalledOnce();
      expect(request.sentAt).toEqual(expect.any(Number));
      expect(request.pendingDuration).toEqual(expect.any(Number));
    });

    it('does not call sendRequest more than once', () => {
      const sendRequest = vi.fn();
      const request = createSocketRequest({ send: sendRequest });

      request.send();
      expect(() => request.send()).toThrow(KafkaJSNonRetriableError);
      expect(sendRequest).toHaveBeenCalledTimes(1);
    });

    it('executes the timeoutHandler and rejects when timeoutRequest is invoked', () => {
      const timeoutHandler = vi.fn();
      const request = createSocketRequest({ timeout: timeoutHandler });
      const rejectedSpy = vi.spyOn(request, 'rejected');

      request.send();
      request.timeoutRequest();

      expect(rejectedSpy).toHaveBeenCalledOnce();
      expect(request.entry.reject).toHaveBeenCalledWith(expect.any(KafkaJSRequestTimeoutError));
      expect(timeoutHandler).toHaveBeenCalledOnce();
    });

    it('times out on its own after requestTimeout elapses once sent', async () => {
      const timeoutHandler = vi.fn();
      const request = createSocketRequest({ requestTimeout: 20, timeout: timeoutHandler });

      request.send();
      await new Promise((resolve) => setTimeout(resolve, 60));

      expect(timeoutHandler).toHaveBeenCalledOnce();
      expect(request.entry.reject).toHaveBeenCalledWith(expect.any(KafkaJSRequestTimeoutError));
    });

    it('never times out when enforceRequestTimeout is false', async () => {
      const timeoutHandler = vi.fn();
      const request = createSocketRequest({
        requestTimeout: 10,
        enforceRequestTimeout: false,
        timeout: timeoutHandler,
      });

      request.send();
      await new Promise((resolve) => setTimeout(resolve, 60));

      expect(timeoutHandler).not.toHaveBeenCalled();
      expect(request.entry.reject).not.toHaveBeenCalled();
    });

    it('does not fire the timeout after the request already completed', async () => {
      const timeoutHandler = vi.fn();
      const request = createSocketRequest({ requestTimeout: 20, timeout: timeoutHandler });

      request.send();
      request.completed({ size, payload });
      await new Promise((resolve) => setTimeout(resolve, 60));

      expect(timeoutHandler).not.toHaveBeenCalled();
    });
  });

  describe('completed', () => {
    it('resolves the promise', () => {
      const request = createSocketRequest();
      expect(request.duration).toBeNull();

      request.send();
      request.completed({ size, payload });

      expect(request.entry.resolve).toHaveBeenCalledWith({
        correlationId: request.correlationId,
        entry: request.entry,
        size,
        payload,
      });
      expect(request.duration).toEqual(expect.any(Number));
    });

    it('does not call resolve more than once', () => {
      const request = createSocketRequest();
      request.send();
      request.completed({ size, payload });
      expect(() => request.completed({ size, payload })).toThrow(KafkaJSNonRetriableError);
      expect(request.entry.resolve).toHaveBeenCalledTimes(1);
    });
  });

  describe('rejected', () => {
    const error = new Error('Test error');

    it('rejects the promise', () => {
      const request = createSocketRequest();
      expect(request.duration).toBeNull();

      request.send();
      request.rejected(error);

      expect(request.entry.reject).toHaveBeenCalledWith(error);
      expect(request.duration).toEqual(expect.any(Number));
    });

    it('does not call reject more than once', () => {
      const request = createSocketRequest();
      request.send();
      request.rejected(error);
      expect(() => request.rejected(error)).toThrow(KafkaJSNonRetriableError);
      expect(request.entry.reject).toHaveBeenCalledTimes(1);
    });

    it('leaves duration null when rejected before ever being sent', () => {
      const request = createSocketRequest();
      request.rejected(error);
      expect(request.duration).toBeNull();
    });
  });

  describe('instrumentation events', () => {
    it('emits NETWORK_REQUEST on completion', () => {
      const emitter = new InstrumentationEventEmitter<NetworkEventMap>();
      const eventCalled = vi.fn();
      emitter.addListener(NETWORK_REQUEST, eventCalled);

      const request = createSocketRequest({ instrumentationEmitter: emitter });
      request.send();
      request.completed({ size, payload });

      expect(eventCalled).toHaveBeenCalledOnce();
      const [event] = eventCalled.mock.calls[0]!;
      expect(event.type).toBe(NETWORK_REQUEST);
      expect(event.payload).toMatchObject({
        apiKey: 0,
        apiName: 'Produce',
        apiVersion: 4,
        broker: 'localhost:9092',
        clientId: 'kafka',
        size,
      });
    });

    it('emits NETWORK_REQUEST_TIMEOUT on timeout', () => {
      const emitter = new InstrumentationEventEmitter<NetworkEventMap>();
      const eventCalled = vi.fn();
      emitter.addListener(NETWORK_REQUEST_TIMEOUT, eventCalled);

      const request = createSocketRequest({ instrumentationEmitter: emitter });
      request.send();
      request.timeoutRequest();

      expect(eventCalled).toHaveBeenCalledOnce();
      const [event] = eventCalled.mock.calls[0]!;
      expect(event.type).toBe(NETWORK_REQUEST_TIMEOUT);
      expect(event.payload).toMatchObject({ apiKey: 0, apiName: 'Produce', apiVersion: 4 });
    });
  });
});
