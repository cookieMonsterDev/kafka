import v8 from 'node:v8';
import vm from 'node:vm';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { KafkaDeliveryTimeoutError, KafkaNonRetriableError, KafkaTimeout } from '../errors';
import { COMPRESSION_TYPES } from '../protocol/compression/index';
import { createRecordAccumulator, type ProduceCall, type RecordAccumulatorOptions } from './record-accumulator';
import type { SendMessagesRequest } from './send-messages';
import type { RecordMetadata } from './types';

const topic = 'topic-name';

function metadataFor(request: SendMessagesRequest): RecordMetadata[] {
  return request.topicMessages.map(({ topic: topicName }) => ({
    topicName,
    partition: 0,
    errorCode: 0,
    baseOffset: 0n,
    logAppendTime: -1n,
    logStartOffset: 0n,
  }));
}

interface DispatchedRequest {
  request: SendMessagesRequest;
  resolve: () => void;
  reject: (error: unknown) => void;
}

/** A dispatch that records each request. It answers at once, or holds every request until settled by hand. */
function recordingDispatch({ hold = false }: { hold?: boolean } = {}) {
  const dispatched: DispatchedRequest[] = [];
  const dispatch = vi.fn(
    (request: SendMessagesRequest) =>
      new Promise<RecordMetadata[]>((resolve, reject) => {
        dispatched.push({ request, resolve: () => resolve(metadataFor(request)), reject });
        if (!hold) resolve(metadataFor(request));
      }),
  );
  const values = (index: number) =>
    dispatched[index]?.request.topicMessages.flatMap(({ messages }) => messages.map(({ value }) => value));
  return { dispatch, dispatched, values };
}

function call(value: string, overrides: Partial<ProduceCall> = {}): ProduceCall {
  return {
    topicMessages: [{ topic, messages: [{ value }] }],
    acks: -1,
    timeout: 30_000,
    bytes: Buffer.byteLength(value),
    ...overrides,
  };
}

function createAccumulator(dispatch: RecordAccumulatorOptions['dispatch'], options: Partial<RecordAccumulatorOptions>) {
  return createRecordAccumulator({
    dispatch,
    lingerMs: 50,
    batchSize: 0,
    bufferMemory: undefined,
    deliveryTimeoutMs: 0,
    maxRequestSize: 1_048_576,
    ...options,
  });
}

/**
 * A full garbage collection where the runtime lets a test force one: Node, but not Bun, where the
 * test using it is skipped. The flag is set at runtime instead of through Vitest's `execArgv` so
 * only this file asks for it; it only exposes `gc` to the new context.
 */
function forcedGarbageCollection(): (() => void) | undefined {
  try {
    v8.setFlagsFromString('--expose-gc');
    const gc: unknown = vm.runInNewContext('gc');
    return typeof gc === 'function' ? (gc as () => void) : undefined;
  } catch {
    return undefined;
  }
}

describe('producer/RecordAccumulator', () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    // Restore spies before the fake timers, or restoring a spy would reinstall a fake setTimeout.
    vi.restoreAllMocks();
    vi.useRealTimers();
  });

  describe('with linger', () => {
    it('dispatches nothing until the linger timer fires', async () => {
      const { dispatch } = recordingDispatch();
      const accumulator = createAccumulator(dispatch, { lingerMs: 50 });

      const sent = accumulator.append(call('a'));
      await vi.advanceTimersByTimeAsync(49);
      expect(dispatch).not.toHaveBeenCalled();

      await vi.advanceTimersByTimeAsync(1);
      await expect(sent).resolves.toHaveLength(1);
      expect(dispatch).toHaveBeenCalledOnce();
    });

    it('merges calls in one linger window into one request, passing their counted bytes', async () => {
      const { dispatch, dispatched, values } = recordingDispatch();
      const accumulator = createAccumulator(dispatch, { lingerMs: 50 });

      const sends = [accumulator.append(call('a', { bytes: 3 })), accumulator.append(call('b', { bytes: 4 }))];
      await vi.advanceTimersByTimeAsync(50);
      await Promise.all(sends);

      expect(dispatch).toHaveBeenCalledOnce();
      expect(values(0)).toEqual(['a', 'b']);
      expect(dispatched[0]?.request).toMatchObject({ topicMessages: [{ topic }], bytes: 7 });
    });

    it('sends calls that differ in acks, compression or timeout as separate requests in parallel', async () => {
      const { dispatch, dispatched } = recordingDispatch({ hold: true });
      const accumulator = createAccumulator(dispatch, { lingerMs: 50 });

      void accumulator.append(call('a', { acks: 1 }));
      void accumulator.append(call('b', { compression: COMPRESSION_TYPES.GZIP }));
      void accumulator.append(call('c', { timeout: 1_000 }));
      void accumulator.append(call('d'));
      await vi.advanceTimersByTimeAsync(50);

      // None has settled, so all were dispatched together.
      expect(dispatched).toHaveLength(4);
    });

    it('resolves each caller with the metadata for its own topics', async () => {
      const { dispatch } = recordingDispatch();
      const accumulator = createAccumulator(dispatch, { lingerMs: 50 });

      const first = accumulator.append(call('a'));
      const second = accumulator.append({
        ...call('b'),
        topicMessages: [{ topic: 'other', messages: [{ value: 'b' }] }],
      });
      await vi.advanceTimersByTimeAsync(50);

      expect(dispatch).toHaveBeenCalledOnce();
      expect((await first).map(({ topicName }) => topicName)).toEqual([topic]);
      expect((await second).map(({ topicName }) => topicName)).toEqual(['other']);
    });

    it('rejects every caller in a request when it fails', async () => {
      const error = new KafkaNonRetriableError('produce failed');
      const dispatch = vi.fn(() => Promise.reject(error));
      const accumulator = createAccumulator(dispatch, { lingerMs: 50 });

      const sends = [accumulator.append(call('a')), accumulator.append(call('b'))];
      const results = Promise.allSettled(sends);
      await vi.advanceTimersByTimeAsync(50);

      expect(await results).toEqual([
        { status: 'rejected', reason: error },
        { status: 'rejected', reason: error },
      ]);
    });

    it('flushes once buffered bytes reach batchSize, without waiting out linger', async () => {
      const { dispatch } = recordingDispatch();
      const accumulator = createAccumulator(dispatch, { lingerMs: 10_000, batchSize: 4 });

      void accumulator.append(call('abc'));
      await vi.advanceTimersByTimeAsync(0);
      expect(dispatch).not.toHaveBeenCalled();

      await accumulator.append(call('d'));
      expect(dispatch).toHaveBeenCalledOnce();
    });

    it('flushes once the buffer holds maxRequestSize bytes, even with batchSize off', async () => {
      const { dispatch } = recordingDispatch();
      const accumulator = createAccumulator(dispatch, { lingerMs: 10_000, maxRequestSize: 4 });

      await accumulator.append(call('abcd'));
      expect(dispatch).toHaveBeenCalledOnce();
    });

    it('flush() sends buffered calls without waiting for the timer', async () => {
      const { dispatch } = recordingDispatch();
      const accumulator = createAccumulator(dispatch, { lingerMs: 10_000 });

      const sent = accumulator.append(call('a'));
      await vi.advanceTimersByTimeAsync(0);
      await accumulator.flush();

      await expect(sent).resolves.toHaveLength(1);
      expect(dispatch).toHaveBeenCalledOnce();
      await accumulator.flush();
      expect(dispatch).toHaveBeenCalledOnce();
    });

    it('sends the chunks of a flush over maxRequestSize one after another, none over the cap', async () => {
      const { dispatch, dispatched, values } = recordingDispatch({ hold: true });
      const accumulator = createAccumulator(dispatch, { lingerMs: 50, maxRequestSize: 12 });

      for (const value of ['abcde', 'fghij', 'klmno']) void accumulator.append(call(value));
      await vi.advanceTimersByTimeAsync(50);
      expect(values(0)).toEqual(['abcde', 'fghij']);
      expect(dispatched).toHaveLength(1);

      dispatched[0]?.resolve();
      await vi.advanceTimersByTimeAsync(0);
      expect(values(1)).toEqual(['klmno']);
    });

    it('keeps one flush in flight, so a later flush cannot overtake it', async () => {
      const { dispatch, dispatched } = recordingDispatch({ hold: true });
      const accumulator = createAccumulator(dispatch, { lingerMs: 50 });

      void accumulator.append(call('a'));
      await vi.advanceTimersByTimeAsync(50);
      void accumulator.append(call('b'));
      await vi.advanceTimersByTimeAsync(500);
      expect(dispatched).toHaveLength(1);

      dispatched[0]?.resolve();
      await vi.advanceTimersByTimeAsync(50);
      expect(dispatched).toHaveLength(2);
    });

    describe('bufferMemory', () => {
      it('rejects a call larger than bufferMemory without dispatching it', async () => {
        const { dispatch } = recordingDispatch();
        const accumulator = createAccumulator(dispatch, { bufferMemory: 4 });

        await expect(accumulator.append(call('abcde'))).rejects.toBeInstanceOf(KafkaNonRetriableError);
        expect(dispatch).not.toHaveBeenCalled();
      });

      it('admits a waiting call once the flush in flight releases its bytes', async () => {
        const { dispatch, dispatched } = recordingDispatch({ hold: true });
        const accumulator = createAccumulator(dispatch, { lingerMs: 50, bufferMemory: 4 });

        const first = accumulator.append(call('abcd'));
        const second = accumulator.append(call('x'));
        await vi.advanceTimersByTimeAsync(50);
        expect(dispatched).toHaveLength(1);

        dispatched[0]?.resolve();
        await expect(first).resolves.toHaveLength(1);
        await vi.advanceTimersByTimeAsync(50);
        dispatched[1]?.resolve();
        await expect(second).resolves.toHaveLength(1);
      });

      it('rejects a call that waits for bufferMemory longer than its timeout', async () => {
        const { dispatch } = recordingDispatch({ hold: true });
        const accumulator = createAccumulator(dispatch, { lingerMs: 10_000, bufferMemory: 4 });

        void accumulator.append(call('abcd'));
        const second = accumulator.append(call('x', { timeout: 50 }));
        // eslint-disable-next-line vitest/valid-expect -- attach the matcher before the fake timer fires
        const assertion = expect(second).rejects.toBeInstanceOf(KafkaTimeout);
        await vi.advanceTimersByTimeAsync(50);
        await assertion;
      });
    });

    describe('delivery deadline', () => {
      it('rejects the caller at its deadline while the request is in flight', async () => {
        const { dispatch, dispatched } = recordingDispatch({ hold: true });
        const accumulator = createAccumulator(dispatch, { lingerMs: 50, deliveryTimeoutMs: 500 });

        const sent = accumulator.append(call('a'));
        // eslint-disable-next-line vitest/valid-expect -- attach the matcher before the fake timer fires
        const assertion = expect(sent).rejects.toBeInstanceOf(KafkaDeliveryTimeoutError);
        await vi.advanceTimersByTimeAsync(500);
        await assertion;
        expect(dispatched[0]?.request.deadline?.reason).toBeInstanceOf(KafkaDeliveryTimeoutError);
      });

      it('holds the next flush until a timed-out request settles', async () => {
        const { dispatch, dispatched } = recordingDispatch({ hold: true });
        const accumulator = createAccumulator(dispatch, { lingerMs: 50, deliveryTimeoutMs: 500 });

        void accumulator.append(call('a')).catch(() => {});
        await vi.advanceTimersByTimeAsync(450);
        const later = accumulator.append(call('b'));
        // The first caller's deadline passes, but its request is still in flight.
        await vi.advanceTimersByTimeAsync(100);
        expect(dispatched).toHaveLength(1);

        dispatched[0]?.reject(new KafkaDeliveryTimeoutError(500));
        await vi.advanceTimersByTimeAsync(50);
        expect(dispatched).toHaveLength(2);
        dispatched[1]?.resolve();
        await expect(later).resolves.toHaveLength(1);
      });

      it('stops a merged request at its earliest caller deadline', async () => {
        const { dispatch, dispatched } = recordingDispatch({ hold: true });
        const accumulator = createAccumulator(dispatch, { lingerMs: 50, deliveryTimeoutMs: 500 });

        void accumulator.append(call('a')).catch(() => {});
        await vi.advanceTimersByTimeAsync(20);
        void accumulator.append(call('b')).catch(() => {});
        await vi.advanceTimersByTimeAsync(30);
        expect(dispatch).toHaveBeenCalledOnce();

        await vi.advanceTimersByTimeAsync(449);
        expect(dispatched[0]?.request.deadline?.reason).toBeUndefined();
        await vi.advanceTimersByTimeAsync(1);
        expect(dispatched[0]?.request.deadline?.reason).toBeInstanceOf(KafkaDeliveryTimeoutError);
      });

      it('does not dispatch a call whose deadline passed while it lingered', async () => {
        const { dispatch } = recordingDispatch();
        const accumulator = createAccumulator(dispatch, { lingerMs: 10_000, deliveryTimeoutMs: 500 });

        const sent = accumulator.append(call('a'));
        // eslint-disable-next-line vitest/valid-expect -- attach the matcher before the fake timer fires
        const assertion = expect(sent).rejects.toBeInstanceOf(KafkaDeliveryTimeoutError);
        await vi.advanceTimersByTimeAsync(10_000);
        await assertion;
        expect(dispatch).not.toHaveBeenCalled();
      });

      it('passes no deadline when deliveryTimeoutMs is not positive', async () => {
        const { dispatch, dispatched } = recordingDispatch();
        const accumulator = createAccumulator(dispatch, { lingerMs: 50, deliveryTimeoutMs: 0 });

        const sent = accumulator.append(call('a'));
        await vi.advanceTimersByTimeAsync(0);
        // Only the linger timer.
        expect(vi.getTimerCount()).toBe(1);
        await vi.advanceTimersByTimeAsync(50);
        await sent;
        expect(dispatched[0]?.request.deadline).toBeUndefined();
      });
    });
  });

  describe('with lingerMs 0', () => {
    it('dispatches a lone call as is on the next microtask, with no timer', async () => {
      const { dispatch, dispatched } = recordingDispatch();
      const accumulator = createAccumulator(dispatch, { lingerMs: 0 });

      const lone = call('a', { bytes: 9 });
      const sent = accumulator.append(lone);
      expect(dispatch).not.toHaveBeenCalled();
      expect(vi.getTimerCount()).toBe(0);

      await Promise.resolve();
      expect(dispatch).toHaveBeenCalledOnce();
      expect(dispatched[0]?.request.topicMessages).toBe(lone.topicMessages);
      expect(dispatched[0]?.request.bytes).toBe(9);
      await expect(sent).resolves.toHaveLength(1);
    });

    it('merges calls made in the same turn into one request', async () => {
      const { dispatch, values } = recordingDispatch();
      const accumulator = createAccumulator(dispatch, { lingerMs: 0 });

      await Promise.all([accumulator.append(call('a')), accumulator.append(call('b'))]);

      expect(dispatch).toHaveBeenCalledOnce();
      expect(values(0)).toEqual(['a', 'b']);
    });

    it('merges only consecutive calls that share request settings, keeping call order', async () => {
      const { dispatch, dispatched, values } = recordingDispatch();
      const accumulator = createAccumulator(dispatch, { lingerMs: 0 });

      await Promise.all([
        accumulator.append(call('a', { acks: 1 })),
        accumulator.append(call('b', { acks: 1 })),
        accumulator.append(call('c')),
        accumulator.append(call('d', { acks: 1 })),
      ]);

      expect(dispatched.map(({ request }) => request.acks)).toEqual([1, -1, 1]);
      expect([0, 1, 2].map(values)).toEqual([['a', 'b'], ['c'], ['d']]);
    });

    it('dispatches every request of a turn at once, none over maxRequestSize', async () => {
      const { dispatch, values } = recordingDispatch({ hold: true });
      const accumulator = createAccumulator(dispatch, { lingerMs: 0, maxRequestSize: 10 });

      for (const value of ['abcde', 'fghij', 'klmno']) void accumulator.append(call(value));
      await Promise.resolve();

      expect([0, 1].map(values)).toEqual([['abcde', 'fghij'], ['klmno']]);
    });

    it('dispatches calls from later turns while earlier requests are in flight', async () => {
      const { dispatch } = recordingDispatch({ hold: true });
      const accumulator = createAccumulator(dispatch, { lingerMs: 0 });

      void accumulator.append(call('a'));
      await Promise.resolve();
      void accumulator.append(call('b'));
      await Promise.resolve();

      expect(dispatch).toHaveBeenCalledTimes(2);
    });

    it('ignores bufferMemory', async () => {
      const { dispatch } = recordingDispatch();
      const accumulator = createAccumulator(dispatch, { lingerMs: 0, bufferMemory: 1 });

      await expect(accumulator.append(call('abcd'))).resolves.toHaveLength(1);
    });

    it('rejects every caller of a request at the deadline, on one timer', async () => {
      const setTimeoutSpy = vi.spyOn(globalThis, 'setTimeout');
      const { dispatch, dispatched } = recordingDispatch({ hold: true });
      const accumulator = createAccumulator(dispatch, { lingerMs: 0, deliveryTimeoutMs: 500 });

      const sends = [accumulator.append(call('a')), accumulator.append(call('b'))];
      // Attach the matchers before the fake timer fires.
      const assertions = sends.map((sent) => expect(sent).rejects.toBeInstanceOf(KafkaDeliveryTimeoutError));
      await vi.advanceTimersByTimeAsync(500);
      await Promise.all(assertions);

      expect(dispatched).toHaveLength(1);
      expect(dispatched[0]?.request.deadline?.reason).toBeInstanceOf(KafkaDeliveryTimeoutError);
      expect(setTimeoutSpy).toHaveBeenCalledOnce();
    });
  });
});

describe('producer/RecordAccumulator once idle', () => {
  const collectGarbage = forcedGarbageCollection();

  it.skipIf(!collectGarbage)(
    'does not keep dispatch reachable through the deadline timer left armed after the last send',
    async () => {
      const dispatchRef = await (async () => {
        const { dispatch } = recordingDispatch();
        const accumulator = createAccumulator(dispatch, { lingerMs: 0, deliveryTimeoutMs: 120_000 });
        await accumulator.append(call('a'));
        return new WeakRef(dispatch);
      })();

      for (let attempt = 0; attempt < 5 && dispatchRef.deref(); attempt++) {
        await new Promise((resolve) => setImmediate(resolve));
        collectGarbage!();
      }
      expect(dispatchRef.deref()).toBeUndefined();
    },
  );
});
