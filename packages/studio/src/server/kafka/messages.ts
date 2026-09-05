import type { Batch, Consumer, KafkaMessage } from '@cookiemonsterdev/kafka-core';
import type {
  MessageRecord,
  MessagesPageResponse,
  MessagesQuery,
  PartitionRange,
} from '../../shared/contracts/message';
import type { PooledAdmin } from './admin-pool';

export type PooledMessageConsumer = Pick<Consumer, 'connect' | 'disconnect' | 'assign' | 'seek' | 'stream'>;

/**
 * Starts `consumer.stream()` and returns it safe to seek against right away: `Consumer.seek()`
 * throws until the consumer's group/assignment state exists, and that state is only created once
 * the stream's first item is requested — requesting (not awaiting) it here forces that setup to
 * happen synchronously instead of on the caller's first `for await` tick. `return()` is forwarded
 * to the underlying generator so an early `break` still runs its cleanup; skipping that leaves the
 * consumer running and `disconnect()` hangs.
 */
export function startMessageStream(
  consumer: PooledMessageConsumer,
  config?: { readonly signal?: AbortSignal },
): AsyncIterable<Batch> {
  const iterator = consumer.stream(config);
  const first = iterator.next();
  let firstConsumed = false;

  return {
    [Symbol.asyncIterator](): AsyncIterator<Batch> {
      return {
        next: (): Promise<IteratorResult<Batch>> => {
          if (firstConsumed) return iterator.next();
          firstConsumed = true;
          return first;
        },
        return: (value?: unknown): Promise<IteratorResult<Batch>> =>
          iterator.return ? iterator.return(value) : Promise.resolve({ done: true, value: undefined }),
      };
    },
  };
}

export interface AssignedCursor {
  readonly partition: number;
  readonly offset: bigint;
}

/**
 * Assigns `consumer` the given partitions and seeks each to its starting offset — assign, start
 * the stream, then seek, the only order {@link startMessageStream} allows. Shared by the bounded
 * page read and the live tail. The caller still owns `consumer.connect()`/`disconnect()`.
 */
export async function openAssignedStream(
  consumer: PooledMessageConsumer,
  topic: string,
  cursors: readonly AssignedCursor[],
  config?: { readonly signal?: AbortSignal },
): Promise<AsyncIterable<Batch>> {
  await consumer.assign(cursors.map((cursor) => ({ topic, partition: cursor.partition })));
  const batches = startMessageStream(consumer, config);
  for (const cursor of cursors) {
    consumer.seek({ topic, partition: cursor.partition, offset: cursor.offset });
  }
  return batches;
}

/** Builds a fresh, unconnected consumer for one profile. Unlike {@link PooledAdmin}, these are never pooled — each request or SSE connection assigns its own partitions and disconnects when done. */
export type MessageConsumerFactory = (profileName: string | null) => { consumer(): PooledMessageConsumer };

function toBase64(value: Buffer | string | null | undefined): string | null {
  if (value === null || value === undefined) return null;
  return Buffer.isBuffer(value) ? value.toString('base64') : Buffer.from(value, 'utf8').toString('base64');
}

function toHeaders(headers: KafkaMessage['headers']): Record<string, string | null> {
  const result: Record<string, string | null> = {};
  for (const [key, raw] of Object.entries(headers)) {
    result[key] = toBase64(Array.isArray(raw) ? raw[raw.length - 1] : raw);
  }
  return result;
}

/** Converts a decoded wire message into the wire-safe shape `MessagesPageResponse`/tail SSE frames send to the browser. */
export function toMessageRecord(partition: number, message: KafkaMessage): MessageRecord {
  return {
    partition,
    offset: message.offset.toString(),
    timestamp: message.timestamp.toString(),
    key: toBase64(message.key),
    value: toBase64(message.value),
    headers: toHeaders(message.headers),
    size: message.byteSize,
  };
}

interface PartitionCursor {
  readonly partition: number;
  readonly start: bigint;
  readonly high: bigint;
}

function toRange(partition: number, low: bigint, high: bigint): PartitionRange {
  return { partition, low: low.toString(), high: high.toString() };
}

function clampOffset(value: bigint, low: bigint, high: bigint): bigint {
  if (value < low) return low;
  if (value > high) return high;
  return value;
}

/** Resolves each target partition's read-start offset from the query's seek mode. */
async function resolveStartOffsets(
  admin: PooledAdmin,
  topic: string,
  query: MessagesQuery,
  targets: readonly { partition: number; low: bigint; high: bigint }[],
): Promise<PartitionCursor[]> {
  if (query.timestamp !== undefined) {
    const seekEntries = await admin.fetchTopicOffsetsByTimestamp(topic, query.timestamp);
    const byPartition = new Map(seekEntries.map((entry) => [entry.partition, entry.offset]));
    return targets.map((range) => {
      const found = byPartition.get(range.partition);
      // Negative offset = the broker's "nothing at or after this timestamp" sentinel.
      const start = found === undefined || found < 0n ? range.high : found;
      return { partition: range.partition, start, high: range.high };
    });
  }

  if (query.from === 'earliest') {
    return targets.map((range) => ({ partition: range.partition, start: range.low, high: range.high }));
  }

  if (query.from !== undefined && query.from !== 'latest') {
    const explicit = clampOffset(BigInt(query.from), 0n, 2n ** 63n - 1n);
    return targets.map((range) => ({
      partition: range.partition,
      start: clampOffset(explicit, range.low, range.high),
      high: range.high,
    }));
  }

  // 'latest' or omitted: the most recent `limit` messages per partition.
  return targets.map((range) => {
    const tailStart = range.high - BigInt(query.limit);
    return { partition: range.partition, start: tailStart > range.low ? tailStart : range.low, high: range.high };
  });
}

/** A read with no explicit seek target: the most recent messages, not history from a fixed point. */
function isTailRead(query: Pick<MessagesQuery, 'timestamp' | 'from'>): boolean {
  return query.timestamp === undefined && (query.from === undefined || query.from === 'latest');
}

function compareByPartitionOffset(a: MessageRecord, b: MessageRecord): number {
  if (a.partition !== b.partition) return a.partition - b.partition;
  const diff = BigInt(a.offset) - BigInt(b.offset);
  return diff < 0n ? -1 : diff > 0n ? 1 : 0;
}

/**
 * Filters out redelivered messages: a `seek()` racing the consumer's own default starting
 * position can hand back one message in two separate batches. Offsets only ever move forward here,
 * so tracking the last one seen per partition catches the repeat.
 */
export class SeenOffsetTracker {
  private readonly lastOffsetByPartition = new Map<number, bigint>();

  /** `true` the first time this partition/offset pair is seen; `false` on a repeat. */
  admit(partition: number, offset: bigint): boolean {
    const last = this.lastOffsetByPartition.get(partition);
    if (last !== undefined && offset <= last) return false;
    this.lastOffsetByPartition.set(partition, offset);
    return true;
  }
}

/** Safety valve against a hung read; every cursor targets an offset already on disk, so this should never trip. */
const PAGE_READ_TIMEOUT_MS = 10_000;

async function collectMessages(
  consumer: PooledMessageConsumer,
  topic: string,
  cursors: readonly PartitionCursor[],
): Promise<MessageRecord[]> {
  const highByPartition = new Map(cursors.map((cursor) => [cursor.partition, cursor.high]));
  const pending = new Set(cursors.filter((cursor) => cursor.start < cursor.high).map((cursor) => cursor.partition));
  if (pending.size === 0) return [];

  const collected: MessageRecord[] = [];
  const seen = new SeenOffsetTracker();
  await consumer.connect();
  try {
    const controller = new AbortController();
    const deadline = setTimeout(() => controller.abort(), PAGE_READ_TIMEOUT_MS);
    try {
      const assigned = cursors.map((cursor) => ({ partition: cursor.partition, offset: cursor.start }));
      const batches = await openAssignedStream(consumer, topic, assigned, { signal: controller.signal });

      for await (const batch of batches) {
        for (const message of batch.messages) {
          if (seen.admit(batch.partition, message.offset)) collected.push(toMessageRecord(batch.partition, message));
        }

        const high = highByPartition.get(batch.partition);
        if (high !== undefined && batch.lastOffset() + 1n >= high) pending.delete(batch.partition);
        if (pending.size === 0) break;
      }
    } finally {
      clearTimeout(deadline);
    }
  } finally {
    await consumer.disconnect();
  }
  return collected;
}

export interface ReadMessagesPageInput {
  readonly topic: string;
  readonly query: MessagesQuery;
}

/** A bounded snapshot read: seeks a short-lived consumer to a computed offset per partition and collects up to `query.limit` messages, then disconnects. Always terminates, unlike the live tail (`./tail`). */
export async function readMessagesPage(
  admin: PooledAdmin,
  consumer: PooledMessageConsumer,
  input: ReadMessagesPageInput,
): Promise<MessagesPageResponse> {
  const { topic, query } = input;
  const allOffsets = await admin.fetchTopicOffsets(topic);
  const ranges = allOffsets.map((offset) => toRange(offset.partition, offset.low, offset.high));

  const targets =
    query.partition === undefined ? allOffsets : allOffsets.filter((o) => o.partition === query.partition);
  if (targets.length === 0) return { messages: [], ranges };

  const cursors = await resolveStartOffsets(admin, topic, query, targets);
  const collected = await collectMessages(consumer, topic, cursors);

  collected.sort(compareByPartitionOffset);
  const limited = isTailRead(query) ? collected.slice(-query.limit) : collected.slice(0, query.limit);

  return { messages: limited, ranges };
}
