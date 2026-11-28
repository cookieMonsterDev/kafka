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
  listTopics(): Promise<string[]>;
  partitionsFor(topic: string): Promise<TopicPartitionInfo[]>;
  clientInstanceId(): Buffer | null;
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
[ProducerConfig](../configuration/#producerconfig).
Apache: [producer configs](https://kafka.apache.org/44/configuration/producer-configs/).

## `send` / `sendBatch`

`ProducerRecord`: `topic`, `messages`, optional `acks`, `timeout`, `compression`,
`compressionLevel`. `ProducerBatch` is the same options with `topicMessages`
for several topics. `compressionLevel` overrides the producer's own default
for that one call; see [Throughput](../../guides/throughput/#compression-level).

### Linger 0 and same-turn sends

`lingerMs: 0` means no added delay, not one Produce request per `send()`.
Calls made in the same event-loop turn, such as several `send()` calls before
the first `await`, leave together once that turn's synchronous code has run.
They go out as one Produce request per broker, split where the records would
exceed `maxRequestSize`. A call with a different `acks`, `timeout`,
`compression`, or `compressionLevel` from the call before it starts a new
request. Every request of the turn starts at once, in call order. An awaited
`send()` on its own, as in a sequential loop, is sent alone.

Calls that share a request share its outcome, as calls batched by a positive
`lingerMs` do:

- A failed request rejects every call in it with the same error.
- The request stops at the earliest call's `deliveryTimeoutMs` deadline.
- Each call resolves with the request's `RecordMetadata` for the topics that
  call sent to.

The Java client and librdkafka also batch records that arrive close together
with `linger.ms=0`.

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
the per-partition Produce error. `logStartOffset` is `-1n`, as in the Java client, when the
negotiated Produce version does not carry it (before v5, Kafka 1.0).

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

A transaction whose `send` or `sendOffsets` failed, or whose `commit()` the
broker rejected, can only be aborted: `commit()` and the next
`producer.transaction()` reject with a `KafkaNonRetriableError` that says so
until you call `abort()`. If `commit()`/`abort()` fails without a broker answer
(for example, the connection dropped), call it again. A producer fenced by a
newer instance with the same `transactionalId` (`PRODUCER_FENCED` or
`INVALID_PRODUCER_EPOCH`) rejects every further transaction call; create a new
producer.

`flush()` sends linger-buffered records. No-op when `lingerMs` is 0.

## `listTopics` / `partitionsFor`

Thin wrappers over cluster Metadata so you do not need Admin for “what topics
exist” or “which partitions does this topic have”:

```ts
await producer.connect();
const topics = await producer.listTopics();
const partitions = await producer.partitionsFor('events');
// [{ topic, partitionId, leader, replicas, isr, offlineReplicas }, ...]
```

`listTopics()` issues Metadata with an empty topic list (all topics the broker
will describe). `partitionsFor(topic)` refreshes that topic’s metadata if stale.

## `clientInstanceId`

KIP-714 UUID assigned by the broker after connect when
[`enableMetricsPush`](../configuration/#kafkaconfig) is on (the default) and the
broker advertises GetTelemetrySubscriptions (Kafka 3.5+). `null` until that
RPC completes, or when telemetry is off / unsupported.

```ts
await producer.connect();
const id = producer.clientInstanceId(); // Buffer | null
```

For load, spread `throughputPreset().producer` into `kafka.producer()` (sticky
partitioner and 32 MiB `bufferMemory`; linger/batch/in-flight are already
constructor defaults). See
[Throughput](../../guides/throughput/) and [Compatibility](../compatibility/).
