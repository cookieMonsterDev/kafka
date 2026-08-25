import { StickyPartitioner } from './producer/partitioners/index';
import type { CustomPartitioner } from './producer/types';

/** Java `buffer.memory` default (32 MiB). Constructor default stays unlimited. */
const BUFFER_MEMORY = 32 * 1024 * 1024;
/** Consume partitions in parallel; constructor/`run` default stays `1`. */
const PARTITIONS_CONSUMED_CONCURRENTLY = 4;

/**
 * Producer options from {@link throughputPreset}. Spread into {@link Kafka.producer}.
 * Linger, batch size, and in-flight caps are constructor defaults; the preset adds sticky
 * partitioning and a 32 MiB `bufferMemory`.
 */
export interface ThroughputPresetProducer {
  createPartitioner: CustomPartitioner;
  bufferMemory: number;
}

/**
 * Consumer `run` options from {@link throughputPreset}. Spread into {@link Consumer.run}.
 * `partitionsConsumedConcurrently` is a run option, not a constructor default.
 */
export interface ThroughputPresetConsumer {
  partitionsConsumedConcurrently: number;
}

/**
 * Named throughput profile. Constructor defaults already batch (`lingerMs: 5`,
 * `batchSize: 16384`, `maxInFlightRequests: 5`). Spread the fragments for sticky
 * partitioning, a 32 MiB send buffer, and consume concurrency:
 *
 * ```ts
 * const { producer, consumer } = throughputPreset()
 * const p = kafka.producer({ ...producer, idempotent: true })
 * await kafka.consumer({ groupId }).run({ ...consumer, eachBatch })
 * ```
 *
 * @see https://kafka.apache.org/43/configuration/producer-configs/
 */
export interface ThroughputPreset {
  producer: ThroughputPresetProducer;
  consumer: ThroughputPresetConsumer;
}

/**
 * Returns sticky-partitioner producer options and consume-concurrency for load-oriented
 * clients. Safe to spread; does not mutate constructor defaults.
 */
export function throughputPreset(): ThroughputPreset {
  return {
    producer: {
      createPartitioner: StickyPartitioner,
      bufferMemory: BUFFER_MEMORY,
    },
    consumer: {
      partitionsConsumedConcurrently: PARTITIONS_CONSUMED_CONCURRENTLY,
    },
  };
}
