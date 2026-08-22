import type { PartitionMetadata } from '../../cluster/index';

function isSamePartitionLeaders(cached: readonly PartitionMetadata[], current: readonly PartitionMetadata[]): boolean {
  if (cached === current) {
    return true;
  }
  if (cached.length !== current.length) {
    return false;
  }
  for (let i = 0; i < current.length; i++) {
    const left = cached[i]!;
    const right = current[i]!;
    if (left.leader !== right.leader || left.partitionId !== right.partitionId) {
      return false;
    }
  }
  return true;
}

/**
 * Caches the `leader >= 0` subset of partition metadata until the topic's
 * metadata identity, length, or leaders change.
 */
export function createAvailablePartitionCache(): {
  available(partitionMetadata: readonly PartitionMetadata[]): readonly PartitionMetadata[];
  invalidate(): void;
} {
  let source: readonly PartitionMetadata[] | undefined;
  let cached: PartitionMetadata[] | undefined;

  return {
    available(partitionMetadata) {
      if (cached !== undefined && source !== undefined && isSamePartitionLeaders(source, partitionMetadata)) {
        return cached;
      }
      source = partitionMetadata;
      cached = partitionMetadata.filter((partition) => partition.leader >= 0);
      return cached;
    },
    invalidate() {
      source = undefined;
      cached = undefined;
    },
  };
}
