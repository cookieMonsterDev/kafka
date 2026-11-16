import { KafkaDeliveryTimeoutError, KafkaNonRetriableError, KafkaTimeout } from '../errors';
import { COMPRESSION_TYPES } from '../protocol/compression/index';
import { DeliveryDeadlines } from './delivery-deadlines';
import { mergeTopicMessages } from './merge-topic-messages';
import type { SendMessagesRequest } from './send-messages';
import type { RecordMetadata } from './types';

/**
 * Sends one Produce request that may merge several calls. Once `request.deadline` expires, no new
 * attempt starts. Settles with the metadata of every partition it wrote to.
 */
export type DispatchRequest = (request: SendMessagesRequest) => Promise<RecordMetadata[]>;

/**
 * One validated `send`/`sendBatch` call, with each topic named once and its record bytes counted.
 * `bytes` must not exceed `maxRequestSize`: a request never splits one call.
 */
export type ProduceCall = Omit<SendMessagesRequest, 'deadline'>;

/** Resolved producer settings; `MessageProducerOptions` documents each one. */
export interface RecordAccumulatorOptions {
  dispatch: DispatchRequest;
  lingerMs: number;
  batchSize: number;
  bufferMemory: number | undefined;
  deliveryTimeoutMs: number;
  maxRequestSize: number;
}

export interface RecordAccumulator {
  /**
   * Holds `call` until it is sent with the calls it can share a Produce request with. Settles with
   * the metadata for its own topics, or rejects with the request's error. Also rejects with
   * `KafkaDeliveryTimeoutError` once its delivery deadline passes, and with lingerMs above 0, with
   * `KafkaNonRetriableError` when `call` alone exceeds `bufferMemory` or `KafkaTimeout` when it
   * waits for `bufferMemory` longer than its `timeout`.
   */
  append: (call: ProduceCall) => Promise<RecordMetadata[]>;
  /** Sends any linger-buffered records immediately and settles once every started flush has. */
  flush: () => Promise<void>;
}

interface PendingSend extends ProduceCall {
  /** `performance.now()` ms at which the caller is rejected with `KafkaDeliveryTimeoutError`. */
  deadlineAt: number;
  resolve: (metadata: RecordMetadata[]) => void;
  reject: (error: unknown) => void;
}

interface BufferWaiter {
  bytes: number;
  resolve: () => void;
  reject: (error: Error) => void;
}

/**
 * Built outside {@link createRecordAccumulator}: the deadline timer stays armed after the last send
 * and holds this factory, and a closure made inside would keep the accumulator, its `dispatch` and
 * so the producer reachable until the timer fires.
 */
function deliveryTimeoutErrorFactory(deliveryTimeoutMs: number): () => Error {
  return () => new KafkaDeliveryTimeoutError(deliveryTimeoutMs);
}

function groupKey(entry: PendingSend): string {
  const compression = entry.compression ?? COMPRESSION_TYPES.None;
  const compressionLevel = entry.compressionLevel ?? '';
  return `${entry.acks}\0${compression}\0${compressionLevel}\0${entry.timeout}`;
}

/**
 * Collects calls into Produce requests and owns when they are sent: linger and size triggers,
 * `bufferMemory` admission, grouping by request settings, `maxRequestSize` chunking, ordering
 * between flushes and the delivery deadline. `dispatch` is the only way out.
 */
export function createRecordAccumulator({
  dispatch,
  lingerMs,
  batchSize,
  bufferMemory,
  deliveryTimeoutMs,
  maxRequestSize,
}: RecordAccumulatorOptions): RecordAccumulator {
  /** Every pending send's delivery deadline, on one timer. */
  const deliveryDeadlines = new DeliveryDeadlines(deliveryTimeoutErrorFactory(deliveryTimeoutMs));
  const bufferLimit =
    lingerMs > 0 && bufferMemory != null && bufferMemory > 0 ? bufferMemory : Number.POSITIVE_INFINITY;
  /** Linger-buffered calls the next flush sends. */
  const pending: PendingSend[] = [];
  /** Bytes of `pending`: what the next flush groups into requests. */
  let pendingBytes = 0;
  /** Bytes admitted under `bufferMemory` and not yet released: `pending` plus flushes still in flight. */
  let reservedBytes = 0;
  const bufferWaiters: BufferWaiter[] = [];
  let lingerTimer: ReturnType<typeof setTimeout> | null = null;
  let flushInProgress: Promise<void> | null = null;
  /** Calls made in the current event-loop turn while lingerMs is 0, sent together on a microtask. */
  let sameTurnSends: PendingSend[] | null = null;

  function deliveryDeadline(): number {
    // Monotonic, so a wall-clock jump moves no deadline.
    return deliveryTimeoutMs > 0 ? performance.now() + deliveryTimeoutMs : Number.POSITIVE_INFINITY;
  }

  /**
   * Races a linger-buffered call against its `deadline`, so the caller sees the rejection as soon
   * as the deadline is up even while an attempt is still in flight. `dispatchUntil` stops the
   * retries themselves.
   */
  function rejectOnDeliveryTimeout<T>(promise: Promise<T>, deadline: number): Promise<T> {
    if (!Number.isFinite(deadline)) return promise;

    return new Promise<T>((resolve, reject) => {
      const tracked = deliveryDeadlines.track(deadline, reject);
      promise.then(
        (value) => {
          deliveryDeadlines.untrack(tracked);
          resolve(value);
        },
        (error: unknown) => {
          deliveryDeadlines.untrack(tracked);
          // eslint-disable-next-line @typescript-eslint/prefer-promise-reject-errors -- forward the send rejection as-is
          reject(error);
        },
      );
    });
  }

  /**
   * Dispatches one request under `deadline`: once it passes, retries stop and the partition gates
   * are released instead of being held by a send nobody is waiting for. The one deadline timer
   * covers it. With `rejectAtDeadline`, the returned promise also rejects then, even while an
   * attempt is in flight. Only a lingerMs 0 send may do that: a linger flush waits for its attempt
   * to settle so the next flush can't overtake it.
   */
  function dispatchUntil(
    call: ProduceCall,
    { deadline, rejectAtDeadline }: { deadline: number; rejectAtDeadline: boolean },
  ): Promise<RecordMetadata[]> {
    const { topicMessages, acks, timeout, compression, compressionLevel, bytes } = call;
    if (!Number.isFinite(deadline)) {
      return dispatch({ topicMessages, acks, timeout, compression, compressionLevel, bytes });
    }
    // Expired while lingering behind an earlier flush: its caller was already rejected.
    if (deadline <= performance.now()) return Promise.reject(new KafkaDeliveryTimeoutError(deliveryTimeoutMs));

    return new Promise<RecordMetadata[]>((resolve, reject) => {
      const tracked = deliveryDeadlines.track(deadline, rejectAtDeadline ? reject : undefined);
      dispatch({ topicMessages, acks, timeout, compression, compressionLevel, bytes, deadline: tracked }).then(
        (metadata) => {
          deliveryDeadlines.untrack(tracked);
          resolve(metadata);
        },
        (error: unknown) => {
          deliveryDeadlines.untrack(tracked);
          // eslint-disable-next-line @typescript-eslint/prefer-promise-reject-errors -- forward the send rejection as-is
          reject(error);
        },
      );
    });
  }

  /**
   * Sends `group` as one merged Produce request and settles each caller with the metadata for its
   * own topics. A merged request shares one fate, so it stops at its earliest caller's deadline.
   */
  async function sendRequest(
    group: readonly PendingSend[],
    { rejectAtDeadline }: { rejectAtDeadline: boolean },
  ): Promise<void> {
    const first = group[0];
    if (!first) return;

    let bytes = 0;
    let deadline = Number.POSITIVE_INFINITY;
    for (const entry of group) {
      bytes += entry.bytes;
      deadline = Math.min(deadline, entry.deadlineAt);
    }

    try {
      const metadata = await dispatchUntil(
        {
          // Each call already names every topic once, in an array of its own.
          topicMessages:
            group.length === 1
              ? first.topicMessages
              : mergeTopicMessages(group.flatMap((entry) => entry.topicMessages)),
          acks: first.acks,
          timeout: first.timeout,
          compression: first.compression,
          compressionLevel: first.compressionLevel,
          bytes,
        },
        { deadline, rejectAtDeadline },
      );
      for (const entry of group) {
        const topics = new Set(entry.topicMessages.map(({ topic }) => topic));
        entry.resolve(metadata.filter((item) => topics.has(item.topicName)));
      }
    } catch (error) {
      for (const entry of group) {
        entry.reject(error);
      }
    }
  }

  /**
   * Packs `entries` into groups whose bytes stay within `maxRequestSize`, in order. Every entry
   * fits alone (a call whose own total exceeds the cap is rejected before it gets here), so this
   * only has to cut a *combined* batch back down - "send what fits, keep the rest for the next
   * request" - never split a single entry.
   */
  function chunkByRequestSize(entries: readonly PendingSend[]): PendingSend[][] {
    const chunks: PendingSend[][] = [];
    let current: PendingSend[] = [];
    let currentBytes = 0;

    for (const entry of entries) {
      if (current.length > 0 && currentBytes + entry.bytes > maxRequestSize) {
        chunks.push(current);
        current = [];
        currentBytes = 0;
      }
      current.push(entry);
      currentBytes += entry.bytes;
    }

    if (current.length > 0) chunks.push(current);
    return chunks;
  }

  // Linger path: admission, triggers and flushes.

  function canAdmit(bytes: number): boolean {
    return reservedBytes + bytes <= bufferLimit;
  }

  function notifyBufferWaiters(): void {
    while (bufferWaiters[0] && canAdmit(bufferWaiters[0].bytes)) {
      const waiter = bufferWaiters.shift();
      if (!waiter) break;
      reservedBytes += waiter.bytes;
      waiter.resolve();
    }
  }

  function releaseReservedBytes(bytes: number): void {
    reservedBytes -= bytes;
    if (reservedBytes < 0) reservedBytes = 0;
    notifyBufferWaiters();
  }

  async function reserveBuffer(bytes: number, timeout: number): Promise<void> {
    if (bytes > bufferLimit) {
      throw new KafkaNonRetriableError(`Record batch of ${bytes} bytes exceeds bufferMemory (${bufferMemory} bytes)`);
    }

    if (canAdmit(bytes)) {
      reservedBytes += bytes;
      return;
    }

    await new Promise<void>((resolve, reject) => {
      const waiter: BufferWaiter = {
        bytes,
        resolve: () => {
          clearTimeout(timer);
          resolve();
        },
        reject: (error: Error) => {
          clearTimeout(timer);
          reject(error);
        },
      };
      const timer = setTimeout(() => {
        const index = bufferWaiters.indexOf(waiter);
        if (index >= 0) bufferWaiters.splice(index, 1);
        waiter.reject(new KafkaTimeout(`Timeout while waiting for producer bufferMemory (${bufferMemory} bytes)`));
      }, timeout);
      bufferWaiters.push(waiter);
      void startFlush();
    });
  }

  function clearLingerTimer(): void {
    if (lingerTimer != null) {
      clearTimeout(lingerTimer);
      lingerTimer = null;
    }
  }

  function scheduleLinger(): void {
    if (lingerTimer != null) return;
    lingerTimer = setTimeout(() => {
      lingerTimer = null;
      void startFlush();
    }, lingerMs);
  }

  function shouldFlushBySize(): boolean {
    if (batchSize > 0 && reservedBytes >= batchSize) return true;
    // Flush as soon as the linger buffer alone holds a full request's worth of bytes, rather
    // than letting it grow well past maxRequestSize while waiting out the rest of lingerMs.
    return pendingBytes >= maxRequestSize;
  }

  async function sendGrouped(entries: readonly PendingSend[]): Promise<void> {
    const groups = new Map<string, PendingSend[]>();
    for (const entry of entries) {
      const key = groupKey(entry);
      const group = groups.get(key);
      if (group) {
        group.push(entry);
      } else {
        groups.set(key, [entry]);
      }
    }

    // Each caller's own `rejectOnDeliveryTimeout` already rejects it on time.
    await Promise.all([...groups.values()].map((group) => sendRequest(group, { rejectAtDeadline: false })));
  }

  async function doFlush(): Promise<void> {
    clearLingerTimer();
    const entries = pending.splice(0);
    const flushedBytes = pendingBytes;
    pendingBytes = 0;
    if (entries.length === 0) return;
    try {
      // Sequential, not Promise.all: chunks can share a topic-partition (two send() calls routed
      // to the same partition), and sending them concurrently would race their sequence-number
      // assignment for idempotent/transactional producers and could reorder delivery generally.
      for (const chunk of chunkByRequestSize(entries)) {
        await sendGrouped(chunk);
      }
    } finally {
      releaseReservedBytes(flushedBytes);
    }
  }

  function startFlush(): Promise<void> {
    if (!flushInProgress) {
      flushInProgress = doFlush().finally(() => {
        flushInProgress = null;
        if (pending.length === 0) return;
        if (shouldFlushBySize()) {
          void startFlush();
        } else {
          scheduleLinger();
        }
      });
    }

    return flushInProgress;
  }

  async function enqueue(call: ProduceCall, deadline: number): Promise<RecordMetadata[]> {
    await reserveBuffer(call.bytes, call.timeout);

    let resolve!: (metadata: RecordMetadata[]) => void;
    let reject!: (error: unknown) => void;
    const result = new Promise<RecordMetadata[]>((res, rej) => {
      resolve = res;
      reject = rej;
    });

    pending.push({ ...call, deadlineAt: deadline, resolve, reject });
    pendingBytes += call.bytes;

    if (shouldFlushBySize()) {
      void startFlush();
    } else {
      scheduleLinger();
    }

    return result;
  }

  // lingerMs 0 path: calls made in one turn leave together, without a timer or one-flush limit.

  /**
   * Splits same-turn calls into requests of consecutive calls that share request settings and fit in
   * `maxRequestSize`, so no call is sent ahead of an earlier one.
   */
  function splitSameTurnRequests(entries: readonly PendingSend[]): PendingSend[][] {
    const runs: PendingSend[][] = [];
    let runKey: string | undefined;
    for (const entry of entries) {
      const key = groupKey(entry);
      const run = runs.at(-1);
      if (run && key === runKey) {
        run.push(entry);
      } else {
        runs.push([entry]);
        runKey = key;
      }
    }
    return runs.flatMap((run) => chunkByRequestSize(run));
  }

  function sendSameTurnSends(): void {
    const entries = sameTurnSends;
    sameTurnSends = null;
    if (!entries) return;

    const [only] = entries;
    if (entries.length === 1 && only) {
      // The common sequential case: send it exactly as an uncoalesced call.
      dispatchUntil(only, { deadline: only.deadlineAt, rejectAtDeadline: true }).then(only.resolve, only.reject);
      return;
    }

    // All requests start now, in call order, as separate calls would: waiting for one to settle
    // before the next would let a later turn's call overtake it on a shared partition.
    for (const request of splitSameTurnRequests(entries)) {
      void sendRequest(request, { rejectAtDeadline: true });
    }
  }

  /**
   * Holds the call until the running synchronous code and the microtasks queued before it have run,
   * so calls made together leave as one Produce per broker without adding a timer.
   */
  function queueSameTurnSend(call: ProduceCall, deadline: number): Promise<RecordMetadata[]> {
    if (!sameTurnSends) {
      sameTurnSends = [];
      queueMicrotask(sendSameTurnSends);
    }
    const entries = sameTurnSends;

    return new Promise<RecordMetadata[]>((resolve, reject) => {
      entries.push({ ...call, deadlineAt: deadline, resolve, reject });
    });
  }

  function append(call: ProduceCall): Promise<RecordMetadata[]> {
    const deadline = deliveryDeadline();
    if (lingerMs <= 0) return queueSameTurnSend(call, deadline);
    return rejectOnDeliveryTimeout(enqueue(call, deadline), deadline);
  }

  async function flush(): Promise<void> {
    while (pending.length > 0 || flushInProgress) {
      await startFlush();
    }
  }

  return { append, flush };
}
