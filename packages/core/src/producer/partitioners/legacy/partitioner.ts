import type { PartitionerArgs } from '../../types';
import { randomBytes } from './random-bytes';

/**
 * A cheap way to deterministically convert a number to a positive value. When the input is
 * positive, the original value is returned. When the input number is negative, the returned
 * positive value is the original value bit AND against 0x7fffffff, which is not its absolute
 * value.
 */
function toPositive(x: number): number {
  return x & 0x7fffffff;
}

export type Murmur2 = (key: Buffer | string | number) => number;

/**
 * The partitioning strategy shared by `default` and `legacy` - both wrap this with their own
 * murmur2 implementation:
 *  - If a partition is specified in the message, use it.
 *  - If no partition is specified but a key is present, choose a partition based on a hash of the key.
 *  - If no partition or key is present, choose a partition in a round-robin fashion.
 *
 * Based on the Java client 0.10.2:
 * https://github.com/apache/kafka/blob/0.10.2/clients/src/main/java/org/apache/kafka/clients/producer/internals/DefaultPartitioner.java
 */
export function createPartitionerFactory(murmur2: Murmur2) {
  return () => {
    const counters = new Map<string, number>();

    return ({ topic, partitionMetadata, message }: PartitionerArgs): number => {
      let counter = counters.get(topic);
      if (counter === undefined) {
        counter = randomBytes(32).readUInt32BE(0);
        counters.set(topic, counter);
      }

      const numPartitions = partitionMetadata.length;
      const availablePartitions = partitionMetadata.filter((p) => p.leader >= 0);
      const numAvailablePartitions = availablePartitions.length;

      if (message.partition != null) {
        return message.partition;
      }

      if (message.key != null) {
        return toPositive(murmur2(message.key)) % numPartitions;
      }

      const nextCounter = toPositive(counter + 1);
      counters.set(topic, nextCounter);

      if (numAvailablePartitions > 0) {
        const i = nextCounter % numAvailablePartitions;
        // `i < availablePartitions.length` by construction, so this index is never `undefined`.
        return availablePartitions[i]!.partitionId;
      }

      // No partitions are available; give a non-available partition.
      return nextCounter % numPartitions;
    };
  };
}
