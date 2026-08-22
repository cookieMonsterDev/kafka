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
  run(config?: ConsumerRunConfig): Promise<void>;
  stream(config?: Omit<ConsumerRunConfig, 'eachBatch' | 'eachMessage'>): AsyncIterableIterator<Batch>;
  stop(): Promise<void>;
  pause(topics: readonly { topic: string; partitions?: number[] }[]): void;
  resume(topics: readonly { topic: string; partitions?: number[] }[]): void;
  paused(): TopicPartitions[];
  seek(topicPartitionOffset: { topic: string; partition: number; offset: bigint | number | string }): void;
  commitOffsets(topicPartitions: readonly TopicPartitionOffsetAndMetadata[]): Promise<void>;
  describeGroup(): Promise<GroupDescription>;
  logger(): Logger;
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

`stream()` cannot run alongside `run()`.

`eachBatch` plus `partitionsConsumedConcurrently` is the heavy-load consume API.
The default concurrency is `1`. Spread `throughputPreset().consumer` into `run()`
for concurrency `4`. See [Throughput](../../guides/throughput/).

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
