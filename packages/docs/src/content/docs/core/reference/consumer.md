---
title: Consumer API
description: Consumer methods, KafkaMessage, and run options
order: 3
section: reference
---

```ts
interface Consumer {
  connect(options?: ConnectOptions): Promise<void>;
  disconnect(options?: ConnectOptions): Promise<void>;
  subscribe(subscription: ConsumerSubscribeTopics | ConsumerSubscribeTopic): Promise<void>;
  assign(topicPartitions: readonly { topic: string; partition: number }[]): Promise<void>;
  run(config?: ConsumerRunConfig): Promise<void>;
  stream(config?: Omit<ConsumerRunConfig, 'eachBatch' | 'eachMessage'>): AsyncIterableIterator<Batch>;
  stop(): Promise<void>;
  pause(topics: readonly { topic: string; partitions?: number[] }[]): void;
  resume(topics: readonly { topic: string; partitions?: number[] }[]): void;
  paused(): TopicPartitions[];
  seek(topicPartitionOffset: { topic: string; partition: number; offset: bigint | number | string }): void;
  commitOffsets(topicPartitions: readonly TopicPartitionOffsetAndMetadata[]): Promise<void>;
  describeGroup(): Promise<GroupDescription>;
  committed(topicPartitions: readonly TopicPartition[]): Promise<TopicPartitionOffsetAndMetadata[]>;
  position(topicPartition: TopicPartition): bigint | null;
  currentLag(topicPartition: TopicPartition): bigint | null;
  logger(): Logger;
  on(eventName: string, listener: (event: unknown) => void | Promise<void>): () => void;
  readonly events: Record<string, string>;
  [Symbol.asyncDispose](): Promise<void>;
}
```

Source: [`consumer/index.ts`](https://github.com/cookieMonsterDev/kafka/blob/master/packages/core/src/consumer/index.ts).
Payload types: [`consumer/types.ts`](https://github.com/cookieMonsterDev/kafka/blob/master/packages/core/src/consumer/types.ts).
Guide: [Consumer](../../guides/consumer/). Config:
[ConsumerConfig](./configuration/#consumerconfig).
Apache: [consumer configs](https://kafka.apache.org/43/configuration/consumer-configs/).

## `subscribe`

```ts
await consumer.subscribe({ topics: ['events'], fromBeginning: true });
await consumer.subscribe({ topic: /^events\./, autoOffsetReset: 'none' });
```

`autoOffsetReset` wins over `fromBeginning` when both are set.

## `assign`

```ts
await consumer.assign([
  { topic: 'events', partition: 0 },
  { topic: 'events', partition: 1 },
]);
```

Fetches exactly these partitions with no group membership (no JoinGroup/SyncGroup, no
`ConsumerGroupHeartbeat`, no rebalancing). Mutually exclusive with `subscribe` on the same
consumer - calling one after the other throws, and so does `run`/`stream` if neither was called.
`groupId` is optional on `kafka.consumer(...)` in this mode; it is required only to call
`commitOffsets`. Guide: [Assign mode](../../guides/consumer/#assign-mode).

## `run` / `stream`

| Option                           | Default | Notes                                                                             |
| -------------------------------- | ------- | --------------------------------------------------------------------------------- |
| `eachMessage`                    |         | Per-record handler                                                                |
| `eachBatch`                      |         | Per-batch handler                                                                 |
| `autoCommit`                     | `true`  | Commit after processing                                                           |
| `autoCommitInterval`             |         | ms. When both interval and threshold are unset, commit after each processed batch |
| `autoCommitThreshold`            |         | messages                                                                          |
| `partitionsConsumedConcurrently` | `1`     | Parallel partitions                                                               |
| `signal`                         |         | Abort to stop                                                                     |
| `onPartitionsRevoked`            |         | Partitions given up this rebalance, before fetching the new assignment            |
| `onPartitionsAssigned`           |         | Partitions newly gained this rebalance                                            |
| `onPartitionsLost`               |         | Fires instead of `onPartitionsRevoked` when the assignment was lost, not revoked  |

`stream()` cannot run alongside `run()`. See
[Rebalance callbacks](../../guides/consumer/#rebalance-callbacks) for the
revoked-vs-lost distinction and error-handling policy.

`eachBatch` plus `partitionsConsumedConcurrently` is the heavy-load consume API.
The default concurrency is `1`. Spread `throughputPreset().consumer` into `run()`
for concurrency `4`. See [Throughput](../../guides/throughput/).

## `committed` / `position` / `currentLag`

```ts
const committed = await consumer.committed([{ topic: 'events', partition: 0 }]);
// [{ topic: 'events', partition: 0, offset: 41n, metadata: null }]

const position = consumer.position({ topic: 'events', partition: 0 }); // 42n | null
const lag = consumer.currentLag({ topic: 'events', partition: 0 }); // bigint | null
```

`committed` reads offsets from the group coordinator (OffsetFetch) and works
whether or not `run()`/`stream()` has started - it queries the broker
directly, the same way `admin.fetchOffsets` does. A partition with no
committed offset comes back as `offset: -1n`, `metadata: null` (Kafka's wire
convention for "none").

`position` is the next fetch offset for a partition currently assigned to
this consumer. It returns `null`, rather than throwing, when the partition
isn't currently assigned - a rebalance can move it away between fetches, for
example. `currentLag` is `highWatermark - position` and returns `null` under
the same condition, or when no Fetch response has landed yet for that
partition. Both throw if the group/assignment hasn't started yet.

## `KafkaMessage`

| Field             | Type             | Notes                                       |
| ----------------- | ---------------- | ------------------------------------------- |
| `offset`          | `bigint`         | Always bigint, including MessageSet on 0.10 |
| `timestamp`       | `bigint`         |                                             |
| `key` / `value`   | `Buffer \| null` |                                             |
| `headers`         | `RecordHeaders`  | Empty on MessageSet                         |
| `isControlRecord` | `boolean`        | Transaction markers                         |
| `magicByte`       | `number`         | 0/1 MessageSet, 2 RecordBatch               |

`seek` / `commitOffsets` still accept `number` and `string` at runtime; prefer
`bigint`.

## ShareConsumer

Returned by `kafka.shareConsumer({ groupId })` (KIP-932). Subscribe is
synchronous. Successful `eachMessage` / auto-acked `eachBatch` calls are
acknowledged as `ACCEPT`; handler failures `RELEASE` the acquired range so
another member can retry. Share fetch runs per assigned node in parallel, with
the same `partitionsConsumedConcurrently` / prefetch knobs as the classic
consumer.

```ts
interface ShareConsumer {
  connect(options?: ConnectOptions): Promise<void>;
  disconnect(options?: ConnectOptions): Promise<void>;
  subscribe(subscription: { topics: readonly string[] }): void;
  run(config: {
    eachMessage?: EachMessageHandler | null;
    eachBatch?: EachShareBatchHandler | null;
    eachBatchAutoAck?: boolean;
    partitionsConsumedConcurrently?: number;
    prefetchMaxBatches?: number;
    prefetchMaxBytes?: number;
  }): Promise<void>;
  stop(): Promise<void>;
  logger(): Logger;
  [Symbol.asyncDispose](): Promise<void>;
}
```

`SHARE_ACKNOWLEDGE_TYPE`: `GAP` 0, `ACCEPT` 1, `RELEASE` 2, `REJECT` 3, `RENEW` 4.
`SHARE_ACQUIRE_MODE`: `BATCH_OPTIMIZED` 0, `RECORD_LIMIT` 1 (ShareFetch v2 / KIP-1206).
Guide: [Share groups](../../guides/consumer/#share-groups-kip-932).
