import type { DebugRecord } from '../../shared/contracts/debug';
import type { GroupPartitionLag } from '../../shared/contracts/group';

export function compactedState(records: readonly DebugRecord[]) {
  const byKey = new Map<string, DebugRecord[]>();
  for (const record of records) {
    if (record.key === null) continue;
    // Keys are scoped to partitions: a partition-count change can put one key in two logs.
    const id = JSON.stringify([record.topic, record.partition, record.key]);
    const history = byKey.get(id) ?? [];
    history.push(record);
    byKey.set(id, history);
  }
  return [...byKey.values()].map((history) => {
    history.sort((a, b) => {
      if (BigInt(a.offset) < BigInt(b.offset)) return -1;
      if (BigInt(a.offset) > BigInt(b.offset)) return 1;
      return 0;
    });
    const latest = history[history.length - 1];
    if (!latest) throw new Error('Empty compacted history');
    return { latest, history, tombstone: latest.value === null };
  });
}

export interface LagSample {
  readonly time: number;
  readonly partitions: readonly GroupPartitionLag[];
}
export function lagDiagnostics(samples: readonly LagSample[]) {
  const first = samples[0];
  const last = samples[samples.length - 1];
  if (!first || !last || last.time <= first.time) return [];
  const seconds = (last.time - first.time) / 1000;
  return last.partitions.map((current) => {
    const previous = first.partitions.find(
      (entry) => entry.topic === current.topic && entry.partition === current.partition,
    );
    const lag = current.lag === null ? null : BigInt(current.lag);
    const history = samples.map((sample) => ({
      time: sample.time,
      partition: sample.partitions.find(
        (entry) => entry.topic === current.topic && entry.partition === current.partition,
      ),
    }));
    const reset = history.some((sample, index) => {
      const before = history[index - 1]?.partition;
      const after = sample.partition;
      if (!before || !after) return false;
      return (
        BigInt(after.logEndOffset) < BigInt(before.logEndOffset) ||
        (before.committedOffset !== null &&
          after.committedOffset !== null &&
          BigInt(after.committedOffset) < BigInt(before.committedOffset))
      );
    });
    let unchangedSince = last.time;
    for (let index = history.length - 1; index >= 0; index--) {
      const sample = history[index];
      if (!sample || sample.partition?.committedOffset !== current.committedOffset) break;
      unchangedSince = sample.time;
    }
    const stalled = lag !== null && lag > 0n && current.committedOffset !== null && last.time - unchangedSince >= 30000;
    if (!previous || current.committedOffset === null || previous.committedOffset === null || lag === null)
      return {
        ...current,
        incomingRate: null,
        committedRate: null,
        catchUpSeconds: null,
        stalled,
        reset,
      };
    const consumed = BigInt(current.committedOffset) - BigInt(previous.committedOffset);
    const incoming = BigInt(current.logEndOffset) - BigInt(previous.logEndOffset);
    const net = consumed - incoming;
    return {
      ...current,
      incomingRate: reset ? null : Number(incoming) / seconds,
      committedRate: reset ? null : Number(consumed) / seconds,
      catchUpSeconds: !reset && net > 0n ? Number(lag) / (Number(net) / seconds) : null,
      stalled,
      reset,
    };
  });
}

/** Group by raw bytes so invalid UTF-8 and null keys never collapse into a made-up hot key. */
export function sampledKeys(records: readonly DebugRecord[]) {
  const counts = new Map<string | null, number>();
  for (const record of records) counts.set(record.key, (counts.get(record.key) ?? 0) + 1);
  return [...counts]
    .map(([keyBase64, count]) => ({ keyBase64, count }))
    .sort((a, b) => b.count - a.count)
    .slice(0, 20);
}
