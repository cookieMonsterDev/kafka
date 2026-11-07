import type { DebugRecord, ScanCoverage, ScanRequest, ScanResult } from '../../shared/contracts/debug';
import { matchesPredicates } from '../../shared/message-query';
import type { PooledAdmin } from './admin-pool';
import { openAssignedStream, SeenOffsetTracker, toMessageRecord, type MessageConsumerFactory } from './messages';

export function searchableRecord(record: DebugRecord) {
  const decode = (value: string | null): string | null =>
    value === null ? null : Buffer.from(value, 'base64').toString('utf8');
  return {
    ...record,
    key: decode(record.key),
    value: decode(record.value),
    headers: Object.fromEntries(Object.entries(record.headers).map(([key, value]) => [key, decode(value)])),
  };
}

/** Snapshot high watermarks never move during a scan. Limits apply across all requested topics. */
export async function scanMessages(
  admin: PooledAdmin,
  factory: MessageConsumerFactory,
  profile: string | null,
  input: ScanRequest,
  signal: AbortSignal,
  onProgress: (result: ScanResult) => void = () => {},
): Promise<ScanResult> {
  const records: DebugRecord[] = [];
  const coverage: (ScanCoverage & { next: string; complete: boolean })[] = [];
  let scanned = 0;
  let matched = 0;
  let bytes = 0;
  let reason: ScanResult['reason'] = 'complete';
  const deadline = AbortSignal.timeout(30000);
  const combined = AbortSignal.any([signal, deadline]);
  const snapshot = (): ScanResult => ({
    records,
    scanned,
    matched,
    coverage,
    reason,
    complete: reason === 'complete' && coverage.every((entry) => entry.complete),
  });
  // Capture every partition before reading so unvisited topics are visible in partial results.
  for (const topic of input.topics) {
    combined.throwIfAborted();
    const bounds = await admin.fetchTopicOffsets(topic);
    const timed = input.startTime === undefined ? [] : await admin.fetchTopicOffsetsByTimestamp(topic, input.startTime);
    for (const bound of bounds) {
      const found = timed.find((entry) => entry.partition === bound.partition)?.offset;
      let start = bound.low;
      if (input.startTime !== undefined) start = found === undefined || found < 0n ? bound.high : found;
      start = start < bound.low ? bound.low : start;
      coverage.push({
        topic,
        partition: bound.partition,
        start: start.toString(),
        end: bound.high.toString(),
        next: start.toString(),
        complete: start >= bound.high,
      });
    }
  }
  try {
    for (const topic of input.topics) {
      if (reason !== 'complete') break;
      combined.throwIfAborted();
      const ranges = coverage.filter((entry) => entry.topic === topic && !entry.complete);
      if (ranges.length === 0) continue;
      const consumer = factory(profile).consumer();
      try {
        await consumer.connect();
        const batches = await openAssignedStream(
          consumer,
          topic,
          ranges.map((entry) => ({ partition: entry.partition, offset: BigInt(entry.start) })),
          { signal: combined },
        );
        const seen = new SeenOffsetTracker();
        for await (const batch of batches) {
          combined.throwIfAborted();
          const range = ranges.find((entry) => entry.partition === batch.partition);
          if (range === undefined || range.complete) continue;
          for (const message of batch.messages) {
            if (
              message.offset < BigInt(range.start) ||
              message.offset >= BigInt(range.end) ||
              !seen.admit(batch.partition, message.offset)
            )
              continue;
            if (scanned >= input.maxRecords) {
              reason = 'record-limit';
              break;
            }
            bytes += message.byteSize;
            if (bytes > 16 * 1024 * 1024) {
              reason = 'byte-limit';
              break;
            }
            const encodeHeader = (value: Buffer | string | null) =>
              value === null ? null : Buffer.from(value).toString('base64');
            const headerValues = Object.fromEntries(
              Object.entries(message.headers).map(([key, value]) => [
                key,
                Array.isArray(value) ? value.map(encodeHeader) : encodeHeader(value),
              ]),
            );
            const record = { topic, ...toMessageRecord(batch.partition, message), headerValues };
            scanned++;
            range.next = (message.offset + 1n).toString();
            const timestamp = BigInt(record.timestamp);
            const inTime =
              (input.startTime === undefined || timestamp >= BigInt(input.startTime)) &&
              (input.endTime === undefined || timestamp <= BigInt(input.endTime));
            if (inTime && matchesPredicates(searchableRecord(record), input.predicates)) {
              matched++;
              records.push(record);
              if (records.length >= input.maxResults) {
                reason = 'result-limit';
                break;
              }
            }
          }
          if (reason === 'complete' && batch.lastOffset() + 1n >= BigInt(range.end)) range.next = range.end;
          range.complete = BigInt(range.next) >= BigInt(range.end);
          onProgress(snapshot());
          if (reason !== 'complete' || ranges.every((entry) => entry.complete)) break;
        }
      } finally {
        await consumer.disconnect();
      }
    }
    if (!coverage.every((entry) => entry.complete) && reason === 'complete') reason = 'timeout';
  } catch (error) {
    if (!combined.aborted) throw error;
    reason = signal.aborted ? 'cancelled' : 'timeout';
  }
  return snapshot();
}
