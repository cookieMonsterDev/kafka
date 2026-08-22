import { StickyPartitioner } from './producer/partitioners/index';
import type { CustomPartitioner } from './producer/types';

/** Java `linger.ms` default since 4.0. Constructor default stays `0`. */
const LINGER_MS = 5;
/** Java `batch.size` default. */
const BATCH_SIZE = 16_384;
/** Java `max.in.flight.requests.per.connection` with idempotence. */
const MAX_IN_FLIGHT_REQUESTS = 5;
/** Java `buffer.memory` default (32 MiB). Constructor default stays unlimited. */
const BUFFER_MEMORY = 32 * 1024 * 1024;
/** Consume partitions in parallel; constructor/`run` default stays `1`. */
const PARTITIONS_CONSUMED_CONCURRENTLY = 4;

/**
 * Producer options from {@link throughputPreset}. Spread into {@link Kafka.producer}.
 * Does not change constructor defaults.
 */
export interface ThroughputPresetProducer {
  lingerMs: number;
  batchSize: number;
  maxInFlightRequests: number;
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
 * Named throughput profile. Constructor defaults stay latency-oriented (`lingerMs: 0`,
 * murmur2, consume concurrency 1). Spread the fragments into producer construction and
 * `consumer.run()`:
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
 * Returns linger/batching/sticky-partitioner producer options and consume-concurrency
 * for load-oriented clients. Safe to spread; does not mutate constructor defaults.
 */
export function throughputPreset(): ThroughputPreset {
  return {
    producer: {
      lingerMs: LINGER_MS,
      batchSize: BATCH_SIZE,
      maxInFlightRequests: MAX_IN_FLIGHT_REQUESTS,
      createPartitioner: StickyPartitioner,
      bufferMemory: BUFFER_MEMORY,
    },
    consumer: {
      partitionsConsumedConcurrently: PARTITIONS_CONSUMED_CONCURRENTLY,
    },
  };
}
