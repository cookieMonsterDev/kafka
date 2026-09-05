import type { MessageRecord } from '../../shared/contracts/message';
import type { SseStream } from '../sse';
import type { PooledAdmin } from './admin-pool';
import { openAssignedStream, SeenOffsetTracker, toMessageRecord, type PooledMessageConsumer } from './messages';

export interface TailFrame {
  readonly message: MessageRecord;
  /** Messages dropped from the buffer just before this one, because production outran delivery. */
  readonly droppedBefore: number;
}

/** Bounded FIFO: pushing past `capacity` drops the oldest entry and counts it, reported on the next delivered frame as a `gap` event instead of silently lost. */
export class BoundedTailQueue {
  private readonly capacity: number;
  private readonly items: MessageRecord[] = [];
  private dropped = 0;

  constructor(capacity: number) {
    this.capacity = capacity;
  }

  push(message: MessageRecord): void {
    this.items.push(message);
    while (this.items.length > this.capacity) {
      this.items.shift();
      this.dropped += 1;
    }
  }

  shift(): TailFrame | undefined {
    const message = this.items.shift();
    if (message === undefined) return undefined;
    const droppedBefore = this.dropped;
    this.dropped = 0;
    return { message, droppedBefore };
  }

  get size(): number {
    return this.items.length;
  }
}

function sleep(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    if (signal.aborted) {
      resolve();
      return;
    }
    const timer = setTimeout(resolve, ms);
    signal.addEventListener('abort', () => resolve(), { once: true });
    signal.addEventListener('abort', () => clearTimeout(timer), { once: true });
  });
}

export interface TailOptions {
  readonly topic: string;
  readonly partition?: number;
  /** Caps how many undelivered messages this connection buffers — see {@link BoundedTailQueue}. */
  readonly maxBuffered: number;
  /** Caps how many `message` frames are written per second, so a burst of production is smoothed rather than flooding the browser in one tick. */
  readonly ratePerSecond?: number;
}

const DEFAULT_RATE_PER_SECOND = 200;

/** Streams newly-produced messages on `topic` (optionally one partition) to `stream` from "now" onward — a tail never replays history, that's the bounded page read (`./messages`). Runs until `signal` aborts or the stream ends. */
export async function runTail(
  admin: PooledAdmin,
  consumer: PooledMessageConsumer,
  stream: Pick<SseStream, 'send'>,
  options: TailOptions,
  signal: AbortSignal,
): Promise<void> {
  const { topic, partition, maxBuffered } = options;
  const ratePerSecond =
    options.ratePerSecond !== undefined && options.ratePerSecond > 0 ? options.ratePerSecond : DEFAULT_RATE_PER_SECOND;
  const delayMs = 1000 / ratePerSecond;

  const offsets = await admin.fetchTopicOffsets(topic);
  const targets = partition === undefined ? offsets : offsets.filter((offset) => offset.partition === partition);
  if (targets.length === 0) return;

  const queue = new BoundedTailQueue(maxBuffered);
  const seen = new SeenOffsetTracker();

  await consumer.connect();
  try {
    const assigned = targets.map((offset) => ({ partition: offset.partition, offset: offset.high }));
    const batches = await openAssignedStream(consumer, topic, assigned, { signal });

    for await (const batch of batches) {
      for (const message of batch.messages) {
        if (seen.admit(batch.partition, message.offset)) queue.push(toMessageRecord(batch.partition, message));
      }

      let frame = queue.shift();
      while (frame !== undefined && !signal.aborted) {
        stream.send('message', frame.message);
        if (frame.droppedBefore > 0) stream.send('gap', { dropped: frame.droppedBefore });
        if (queue.size > 0) await sleep(delayMs, signal);
        frame = queue.shift();
      }
    }
  } finally {
    await consumer.disconnect();
  }
}
