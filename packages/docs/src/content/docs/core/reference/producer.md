---
title: Producer API
description: Producer methods, Message, and RecordMetadata
order: 2
section: reference
---

```ts
interface Producer {
  connect(options?: ConnectOptions): Promise<void>;
  disconnect(options?: ConnectOptions): Promise<void>;
  send(record: ProducerRecord & { signal?: AbortSignal }): Promise<RecordMetadata[]>;
  sendBatch(batch: ProducerBatch & { signal?: AbortSignal }): Promise<RecordMetadata[]>;
  flush(): Promise<void>;
  transaction(): Promise<Transaction>;
  isIdempotent(): boolean;
  on(eventName: string, listener: (event: unknown) => void | Promise<void>): () => void;
  readonly events: Record<string, string>;
  logger(): Logger;
  [Symbol.asyncDispose](): Promise<void>;
}
```

Source: [`producer/index.ts`](https://github.com/cookieMonsterDev/kafka/blob/master/packages/core/src/producer/index.ts).
Types: [`producer/types.ts`](https://github.com/cookieMonsterDev/kafka/blob/master/packages/core/src/producer/types.ts).
Guide: [Producer](../../guides/producer/). Config:
[ProducerConfig](./configuration/#producerconfig).
Apache: [producer configs](https://kafka.apache.org/43/configuration/producer-configs/).

## `send` / `sendBatch`

`ProducerRecord`: `topic`, `messages`, optional `acks`, `timeout`, `compression`,
`compressionLevel`. `ProducerBatch` is the same options with `topicMessages`
for several topics. `compressionLevel` overrides the producer's own default
for that one call; see [Throughput](../../guides/throughput/#compression-level).

### `Message`

| Field       | Type                       | Notes                                 |
| ----------- | -------------------------- | ------------------------------------- |
| `key`       | `Buffer \| string \| null` | Optional                              |
| `value`     | `Buffer \| string \| null` | Required                              |
| `partition` | `number`                   | Optional explicit partition           |
| `headers`   | `RecordHeaders`            | Kafka 0.11+ (Produce v3)              |
| `timestamp` | `number`                   | ms since epoch; defaults to send time |

### `RecordMetadata`

`baseOffset`, `logAppendTime`, and `logStartOffset` are `bigint`. `errorCode` is
the per-partition Produce error.

## `transaction`

Requires `transactionalId` on the producer (InitProducerId, Kafka 0.11+).

```ts
interface Transaction {
  send(record: ProducerRecord & { signal?: AbortSignal }): Promise<RecordMetadata[]>;
  sendBatch(batch: ProducerBatch & { signal?: AbortSignal }): Promise<RecordMetadata[]>;
  sendOffsets(options: { consumerGroupId: string; topics: readonly TopicOffsets[] }): Promise<void>;
  commit(): Promise<void>;
  abort(): Promise<void>;
  isActive(): boolean;
}
```

`flush()` sends linger-buffered records. No-op when `lingerMs` is 0.

For load, spread `throughputPreset().producer` into `kafka.producer()` (`lingerMs: 5`,
`batchSize: 16384`, `maxInFlightRequests: 5`, sticky partitioner). See
[Throughput](../../guides/throughput/) and [Compatibility](./compatibility/).
