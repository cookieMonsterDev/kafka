// Helpers several adapters need. Library-free so every adapter can use them.

import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import type { WireSettings } from '../spec.ts';

function readManifestVersion(directory: string, packageName: string): string | null {
  try {
    const manifest = JSON.parse(readFileSync(join(directory, 'package.json'), 'utf8')) as {
      name?: unknown;
      version?: unknown;
    };
    if (manifest.name === packageName && typeof manifest.version === 'string') {
      return manifest.version;
    }
    return null;
  } catch {
    return null;
  }
}

/**
 * Version of the installed `packageName`. Walks up from the resolved entry point, because a
 * package's `exports` map may hide its `package.json`.
 */
export function readInstalledVersion(packageName: string): string {
  let directory = dirname(fileURLToPath(import.meta.resolve(packageName)));
  while (dirname(directory) !== directory) {
    const version = readManifestVersion(directory, packageName);
    if (version !== null) {
      return version;
    }
    directory = dirname(directory);
  }
  throw new Error(`cannot find the package.json of ${packageName}`);
}

/** Offset Kafka stores as committed after `lastOffset`: the offset of the next record to read. */
export function nextOffsetAfter(lastOffset: bigint | string): bigint {
  return BigInt(lastOffset) + 1n;
}

/** Sum of value lengths, for `ConsumeOptions.onBatch`; a null value counts as 0 bytes. */
export function sumValueBytes(messages: readonly { value: Buffer | null }[]): number {
  let valueBytes = 0;
  for (const message of messages) {
    valueBytes += message.value === null ? 0 : message.value.length;
  }
  return valueBytes;
}

/**
 * Commit policy for `ConsumeOptions.commitEvery`, shared so every client commits at the same points:
 * after every N delivered records, the position after the last record of each partition seen since
 * the previous commit, in one request that delivery waits for.
 */
export interface OffsetCommitter<LastRecord> {
  /** Remembers `record` as the last delivered record of `partition`; one committer serves one topic. */
  seen(partition: number, record: LastRecord): void;
  /** Counts delivered records; returns the commit to await once `commitEvery` were delivered, otherwise null. */
  delivered(count: number): Promise<void> | null;
}

/**
 * `commit` receives the last record of each partition seen since the previous commit and must commit
 * `nextOffsetAfter` its offset. Records are kept as is so the hot path allocates nothing per message.
 */
export function createOffsetCommitter<LastRecord>(
  commitEvery: number,
  commit: (lastRecords: ReadonlyMap<number, LastRecord>) => Promise<void>,
): OffsetCommitter<LastRecord> {
  let lastRecords = new Map<number, LastRecord>();
  let deliveredSinceCommit = 0;
  return {
    seen(partition, record) {
      lastRecords.set(partition, record);
    },
    delivered(count) {
      deliveredSinceCommit += count;
      if (deliveredSinceCommit < commitEvery) {
        return null;
      }
      const dueRecords = lastRecords;
      lastRecords = new Map();
      deliveredSinceCommit = 0;
      return commit(dueRecords);
    },
  };
}

/**
 * Gaps of a producer that turns each `send()` call into requests at once, with no linger and no
 * byte-based batching (kafkajs, Platformatic).
 */
export function perSendBatchingGaps(wire: WireSettings): string[] {
  const gaps = ['batchBytes: no byte-based batching; a produce batch is exactly the records of one send() call'];
  if (wire.lingerMs > 0) {
    gaps.push(`lingerMs: no linger; each send() call is produced at once instead of waiting ${wire.lingerMs} ms`);
  }
  return gaps;
}
