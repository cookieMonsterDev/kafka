import type { PartitionMetadata } from '../../../cluster/index';
import type { NodeLatencyReader } from '../../node-latency-tracker';
import type { Partitioner, PartitionerArgs } from '../../types';
import { createAvailablePartitionCache } from '../available-partitions';
import { murmur2 } from '../default/murmur2';
import { randomBytes } from '../legacy/random-bytes';

interface StickyState {
  partition: number;
  rotate: boolean;
}

export interface StickyPartitionerOptions {
  /**
   * Bias rotation toward the candidate whose leader has the lowest recorded Produce latency,
   * instead of picking uniformly at random. Falls back to the plain KIP-794 uniform choice
   * whenever no latency data is available yet (e.g. a node that's never been produced to).
   * Default `true`.
   * @see https://kafka.apache.org/43/configuration/producer-configs/#partitioner.adaptive.partitioning.enable
   */
  adaptive?: boolean;
}

function toPositive(value: number): number {
  return value & 0x7fffffff;
}

function randomIndex(length: number): number {
  return randomBytes(4).readUInt32BE(0) % length;
}

/** KIP-794's plain rule: uniformly at random from every candidate except the previous partition. */
function chooseUniform(candidates: readonly PartitionMetadata[], previousIndex: number): number {
  if (previousIndex < 0) return candidates[randomIndex(candidates.length)]!.partitionId;

  const index = randomIndex(candidates.length - 1);
  return candidates[index === previousIndex ? candidates.length - 1 : index]!.partitionId;
}

/** Weighted-random pick over `pool`, weight `1 / (1 + latencyMs)` — lower latency, higher odds. */
function chooseWeighted(pool: readonly PartitionMetadata[], weights: readonly number[]): number {
  const total = weights.reduce((sum, weight) => sum + weight, 0);
  const target = (randomBytes(4).readUInt32BE(0) / 0x1_0000_0000) * total;

  let cumulative = 0;
  for (let i = 0; i < pool.length; i++) {
    cumulative += weights[i]!;
    if (target < cumulative) return pool[i]!.partitionId;
  }
  return pool[pool.length - 1]!.partitionId; // floating-point rounding fallback
}

function choosePartition(
  candidates: readonly PartitionMetadata[],
  previous: number | undefined,
  adaptive: boolean,
  nodeLatency: NodeLatencyReader | undefined,
): number {
  if (candidates.length === 1) return candidates[0]!.partitionId;

  const previousIndex = previous == null ? -1 : candidates.findIndex(({ partitionId }) => partitionId === previous);

  if (!adaptive || !nodeLatency) {
    return chooseUniform(candidates, previousIndex);
  }

  const pool = previousIndex < 0 ? candidates : candidates.filter((_candidate, index) => index !== previousIndex);
  const latencies = pool.map(({ leader }) => nodeLatency.latencyFor(leader));
  if (latencies.every((latency) => latency == null)) {
    // No signal for any candidate yet - stay strictly uniform rather than fabricate a bias.
    return chooseUniform(candidates, previousIndex);
  }

  const weights = latencies.map((latency) => 1 / (1 + (latency ?? 0)));
  return chooseWeighted(pool, weights);
}

/**
 * KIP-794 uniform sticky partitioner.
 *
 * Explicit partitions are honored, keyed records retain Java-compatible murmur2
 * routing, and unkeyed records stay on one available partition for a producer
 * batch before rotating to a different partition. Rotation defaults to biasing
 * toward whichever candidate's leader has responded fastest so far (`adaptive`,
 * default `true`; pass `{ adaptive: false }` for the plain KIP-794 uniform choice).
 *
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-794%3A+Strictly+Uniform+Sticky+Partitioner
 */
export const StickyPartitioner: (options?: StickyPartitionerOptions) => Partitioner = ({
  adaptive = true,
}: StickyPartitionerOptions = {}) => {
  const states = new Map<string, StickyState>();
  const availablePartitions = createAvailablePartitionCache();

  const candidatesFor = (partitionMetadata: readonly PartitionMetadata[]): readonly PartitionMetadata[] => {
    const available = availablePartitions.available(partitionMetadata);
    return available.length > 0 ? available : partitionMetadata;
  };

  const partitioner: Partitioner = ({ topic, partitionMetadata, message, nodeLatency }: PartitionerArgs): number => {
    if (message.partition != null) return message.partition;

    if (message.key != null) {
      return toPositive(murmur2(message.key)) % partitionMetadata.length;
    }

    const candidates = candidatesFor(partitionMetadata);
    const state = states.get(topic);
    if (!state) {
      const partition = choosePartition(candidates, undefined, adaptive, nodeLatency);
      states.set(topic, { partition, rotate: false });
      return partition;
    }

    const partitionStillExists = candidates.some(({ partitionId }) => partitionId === state.partition);
    if (state.rotate || !partitionStillExists) {
      state.partition = choosePartition(candidates, state.partition, adaptive, nodeLatency);
      state.rotate = false;
    }

    return state.partition;
  };

  partitioner.onNewBatch = ({ topic, partitionMetadata }) => {
    const state = states.get(topic);
    if (state) state.rotate = true;
    availablePartitions.invalidate();
    availablePartitions.available(partitionMetadata);
  };

  return partitioner;
};
