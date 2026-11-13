import { afterEach, describe, expect, it, vi } from 'vitest';
import { KafkaNonRetriableError, KafkaRequestTimeoutError } from '../../errors';
import { InstrumentationEventEmitter } from '../../instrumentation/emitter';
import { createLogger, LOG_LEVELS } from '../../loggers/index';
import { NETWORK_REQUEST, NETWORK_REQUEST_TIMEOUT } from '../instrumentation-events';
import type { NetworkEventMap } from '../instrumentation-events';
import { SocketRequest } from './socket-request';
import type { RequestEntry, SocketRequestOptions } from './socket-request';

const silentLogger = createLogger({ level: LOG_LEVELS.NOTHING, logCreator: () => () => {} });

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
      logger: silentLogger,
      ...overrides,
    });

  afterEach(() => {
    vi.useRealTimers();
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
      expect(() => request.send()).toThrow(KafkaNonRetriableError);
      expect(sendRequest).toHaveBeenCalledTimes(1);
    });

    it('executes the timeoutHandler and rejects when timeoutRequest is invoked', () => {
      const timeoutHandler = vi.fn();
      const request = createSocketRequest({ timeout: timeoutHandler });
      const rejectedSpy = vi.spyOn(request, 'rejected');

      request.send();
      request.timeoutRequest();

      expect(rejectedSpy).toHaveBeenCalledOnce();
      expect(request.entry.reject).toHaveBeenCalledWith(expect.any(KafkaRequestTimeoutError));
      expect(timeoutHandler).toHaveBeenCalledOnce();
    });

    it('times out on its own after requestTimeout elapses once sent', async () => {
      const timeoutHandler = vi.fn();
      const request = createSocketRequest({ requestTimeout: 20, timeout: timeoutHandler });

      request.send();
      await new Promise((resolve) => setTimeout(resolve, 60));

      expect(timeoutHandler).toHaveBeenCalledOnce();
      expect(request.entry.reject).toHaveBeenCalledWith(expect.any(KafkaRequestTimeoutError));
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

  describe('request timeout', () => {
    it('rejects with the request timing fields once requestTimeout elapses', async () => {
      vi.useFakeTimers();
      const timeoutHandler = vi.fn();
      const request = createSocketRequest({ requestTimeout: 200, timeout: timeoutHandler });

      request.send();
      await vi.advanceTimersByTimeAsync(199);
      expect(timeoutHandler).not.toHaveBeenCalled();

      await vi.advanceTimersByTimeAsync(1);
      expect(timeoutHandler).toHaveBeenCalledOnce();
      const error = vi.mocked(request.entry.reject).mock.calls[0]?.[0] as KafkaRequestTimeoutError;
      expect(error).toBeInstanceOf(KafkaRequestTimeoutError);
      expect(error.message).toBe('Request Produce(key: 0, version: 4) timed out');
      expect(error).toMatchObject({
        broker: 'localhost:9092',
        correlationId: request.correlationId,
        createdAt: request.createdAt,
        sentAt: request.sentAt,
        pendingDuration: request.pendingDuration,
      });
      expect(vi.getTimerCount()).toBe(0);
    });

    it('arms no timer until the request is sent', () => {
      vi.useFakeTimers();
      createSocketRequest();
      expect(vi.getTimerCount()).toBe(0);
    });

    it('clears its timer when the request completes', () => {
      vi.useFakeTimers();
      const request = createSocketRequest();

      request.send();
      expect(vi.getTimerCount()).toBe(1);
      request.completed({ size, payload });
      expect(vi.getTimerCount()).toBe(0);
    });

    it('clears its timer when the request is rejected', () => {
      vi.useFakeTimers();
      const request = createSocketRequest();

      request.send();
      request.rejected(new Error('closed'));
      expect(vi.getTimerCount()).toBe(0);
    });

    it('arms no timer when enforceRequestTimeout is false', () => {
      vi.useFakeTimers();
      const request = createSocketRequest({ enforceRequestTimeout: false });

      request.send();
      expect(vi.getTimerCount()).toBe(0);
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
      expect(() => request.completed({ size, payload })).toThrow(KafkaNonRetriableError);
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
      expect(() => request.rejected(error)).toThrow(KafkaNonRetriableError);
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

    it('passes a lazy NETWORK_REQUEST payload so nothing is built without listeners', () => {
      const emitter = new InstrumentationEventEmitter<NetworkEventMap>();
      const emit = vi.spyOn(emitter, 'emit');

      const request = createSocketRequest({ instrumentationEmitter: emitter });
      request.send();
      request.completed({ size, payload });

      expect(emit).toHaveBeenCalledWith(NETWORK_REQUEST, expect.any(Function));
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

    it('logs a throwing listener instead of propagating it', () => {
      const emitter = new InstrumentationEventEmitter<NetworkEventMap>();
      emitter.addListener(NETWORK_REQUEST, () => {
        throw new Error('listener blew up');
      });
      const error = vi.fn();
      const logger = { ...silentLogger, error };

      const request = createSocketRequest({ instrumentationEmitter: emitter, logger });
      request.send();

      expect(() => request.completed({ size, payload })).not.toThrow();
      expect(request.entry.resolve).toHaveBeenCalledOnce();
      expect(error).toHaveBeenCalledWith(
        'Instrumentation listener failed',
        expect.objectContaining({ eventName: NETWORK_REQUEST, error: 'listener blew up' }),
      );
    });
  });
});
