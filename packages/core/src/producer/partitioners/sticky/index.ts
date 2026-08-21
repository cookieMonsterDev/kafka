import type { PartitionMetadata } from '../../../cluster/index';
import type { CustomPartitioner, Partitioner, PartitionerArgs } from '../../types';
import { murmur2 } from '../default/murmur2';
import { randomBytes } from '../legacy/random-bytes';

interface StickyState {
  partition: number;
  rotate: boolean;
}

function toPositive(value: number): number {
  return value & 0x7fffffff;
}

function randomIndex(length: number): number {
  return randomBytes(4).readUInt32BE(0) % length;
}

function choosePartition(partitionMetadata: readonly PartitionMetadata[], previous?: number): number {
  const available = partitionMetadata.filter(({ leader }) => leader >= 0);
  const candidates = available.length > 0 ? available : partitionMetadata;

  if (candidates.length === 1) return candidates[0]!.partitionId;

  const previousIndex = previous == null ? -1 : candidates.findIndex(({ partitionId }) => partitionId === previous);
  if (previousIndex < 0) return candidates[randomIndex(candidates.length)]!.partitionId;

  // KIP-794 chooses uniformly from every candidate except the previous partition.
  const index = randomIndex(candidates.length - 1);
  return candidates[index === previousIndex ? candidates.length - 1 : index]!.partitionId;
}

/**
 * KIP-794 uniform sticky partitioner.
 *
 * Explicit partitions are honored, keyed records retain Java-compatible murmur2
 * routing, and unkeyed records stay on one available partition for a producer
 * batch before rotating uniformly to a different partition.
 *
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-794%3A+Strictly+Uniform+Sticky+Partitioner
 */
export const StickyPartitioner: CustomPartitioner = () => {
  const states = new Map<string, StickyState>();

  const partitioner: Partitioner = ({ topic, partitionMetadata, message }: PartitionerArgs): number => {
    if (message.partition != null) return message.partition;

    if (message.key != null) {
      return toPositive(murmur2(message.key)) % partitionMetadata.length;
    }

    const state = states.get(topic);
    if (!state) {
      const partition = choosePartition(partitionMetadata);
      states.set(topic, { partition, rotate: false });
      return partition;
    }

    const partitionStillExists = partitionMetadata.some(({ partitionId }) => partitionId === state.partition);
    if (state.rotate || !partitionStillExists) {
      state.partition = choosePartition(partitionMetadata, state.partition);
      state.rotate = false;
    }

    return state.partition;
  };

  partitioner.onNewBatch = ({ topic }) => {
    const state = states.get(topic);
    if (state) state.rotate = true;
  };

  return partitioner;
};
