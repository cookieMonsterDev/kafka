import type { PartitionerArgs } from '../../types';
import { createAvailablePartitionCache } from '../available-partitions';
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
    const availablePartitions = createAvailablePartitionCache();

    return ({ topic, partitionMetadata, message }: PartitionerArgs): number => {
      let counter = counters.get(topic);
      if (counter === undefined) {
        counter = randomBytes(32).readUInt32BE(0);
        counters.set(topic, counter);
      }

      if (message.partition != null) {
        return message.partition;
      }

      const numPartitions = partitionMetadata.length;
      if (message.key != null) {
        return toPositive(murmur2(message.key)) % numPartitions;
      }

      const nextCounter = toPositive(counter + 1);
      counters.set(topic, nextCounter);

      const available = availablePartitions.available(partitionMetadata);
      if (available.length > 0) {
        const i = nextCounter % available.length;
        return available[i]!.partitionId;
      }

      return nextCounter % numPartitions;
    };
  };
}
