---
title: Producer
description: send, sendBatch, acks, linger, partitioners, and abort
order: 1
section: guides
---

```ts
import { Kafka, CompressionTypes, Partitioners } from '@cookiemonsterdev/kafka-core';

const kafka = new Kafka({ clientId: 'my-app', brokers: ['localhost:9092'] });
const producer = kafka.producer({
  acks: -1,
  compression: CompressionTypes.GZIP,
});

await producer.connect();
const metadata = await producer.send({
  topic: 'events',
  messages: [{ key: 'user-1', value: 'hello', headers: { 'x-trace': '1' } }],
});
console.log(metadata[0]?.baseOffset); // bigint
await producer.disconnect();
```

Methods live on [`Producer`](../../reference/producer/). Config fields are in
[Configuration](../../reference/configuration/). Source:
[`producer/index.ts`](https://github.com/cookieMonsterDev/kafka/blob/master/packages/core/src/producer/index.ts).

## Message shape

Each `Message` can set `key`, `value`, `headers`, `partition`, and `timestamp`.
`value` may be `string`, `Buffer`, or `null`. See
[Messages](https://kafka.apache.org/43/implementation/messages/).

## Acks, linger, and batching

Producer-level `acks` (default `-1`, all ISR) and `compression` apply when a
call omits them. `lingerMs` defaults to `0`, so each `send()` is its own Produce
request — latency-first. Set `lingerMs` / `batchSize` to batch, or spread
[`throughputPreset()`](./throughput/). The **next major** of this client will
default `lingerMs` to 5 (see
[Breaking changes](../../migration/breaking-changes/)). See
[producer configs](https://kafka.apache.org/43/configuration/producer-configs/).

GZIP, Snappy, LZ4, and ZSTD are built in (`CompressionTypes.GZIP` / `.Snappy` /
`.LZ4` / `.ZSTD`). Kafka Snappy uses xerial snappy-java framing. LZ4 uses the
LZ4 Frame format (LZ4F) that Apache Kafka writes for magic-2 record batches.
ZSTD needs Kafka 2.1+ (Produce v7). GZIP and ZSTD use Node’s zlib threadpool.
Snappy and LZ4 run off-thread (`worker_threads`; optional native packages if
installed). Built-in codecs remain overridable via `CompressionCodecs`.
Prefer GZIP or ZSTD under load; Snappy and LZ4 are also safe for the event
loop now that they are off-thread.

## Delivery timeout and retries

`deliveryTimeoutMs` (default `120000`) is an end-to-end deadline for one
`send()` / `sendBatch()` call — it covers `lingerMs`, any wait for
`bufferMemory` to free up, and every retry attempt together, not any single
RPC. Once it elapses the call rejects with `KafkaDeliveryTimeoutError`,
regardless of how many retries `retry` still has left; the in-flight attempt,
if any, is not cancelled, so a late response can still land on the broker
after the caller has already moved on. Pass `0` to disable it.

This is a different knob from the per-call `timeout` on `send()` /
`sendBatch()` (default `30000`), which is the wire-level deadline the _broker_
uses for that one Produce request before it responds — it doesn't bound
retries or linger at all. Keep `deliveryTimeoutMs` comfortably above
`lingerMs` plus that per-call `timeout`, and above `retry.maxRetryTime`
times however many retries you actually expect to need, or the deadline can
fire while a perfectly healthy retry is still in flight.

```ts
import { KafkaDeliveryTimeoutError } from '@cookiemonsterdev/kafka-core';

const producer = kafka.producer({ deliveryTimeoutMs: 30_000, retry: { retries: 3 } });

try {
  await producer.send({ topic: 'events', messages: [{ value: 'hello' }] });
} catch (error) {
  if (error instanceof KafkaDeliveryTimeoutError) {
    // Gave up waiting - the message may or may not have reached the broker.
  }
}
```

## maxRequestSize

`maxRequestSize` (default `1_048_576`, 1 MiB) caps the uncompressed bytes of
one Produce request. It's enforced client-side, before a record ever occupies
a linger slot or reaches the network — a different failure than the broker's
`MESSAGE_TOO_LARGE` protocol error, which only fires after the broker has
already accepted bytes on the wire (see [Errors](../../reference/errors/)).

Two checks apply:

- A single record whose own size already exceeds `maxRequestSize` rejects
  immediately at the `send()` / `sendBatch()` call, with
  `KafkaMessageTooLargeError`. So does a call whose records, combined, exceed
  the cap even though none alone does — this client doesn't split one call's
  records across multiple requests.
- With `lingerMs` and `batchSize` batching multiple calls together, a
  combined batch that would otherwise exceed `maxRequestSize` is sent as
  multiple Produce requests instead of one oversized one — each call's own
  records land in one request, in order, none over the cap.

```ts
import { KafkaMessageTooLargeError } from '@cookiemonsterdev/kafka-core';

const producer = kafka.producer({ maxRequestSize: 1_048_576 });

try {
  await producer.send({ topic: 'events', messages: [{ value: hugePayload }] });
} catch (error) {
  if (error instanceof KafkaMessageTooLargeError) {
    // error.size / error.maxRequestSize - split the payload or raise the cap.
  }
}
```

Keep `maxRequestSize` at or above `batchSize` — a `batchSize` larger than
`maxRequestSize` still triggers a flush before the buffer grows past the cap,
but records will spend less time batching. Compression happens per Produce
request, after this check, so `maxRequestSize` measures the same uncompressed
bytes `bufferMemory` accounts for, not the compressed request actually sent
over the wire.

## Partitioners

The default is murmur2 (`Partitioners.DefaultPartitioner`). Pass
`createPartitioner: Partitioners.LegacyPartitioner` for pre-2.0 key routing.
For KIP-794 uniform sticky routing, opt in with
`createPartitioner: Partitioners.StickyPartitioner`. Explicit partitions are
honored, keyed records use murmur2 routing, and unkeyed records share a
partition for each producer batch before rotating. The default
remains unchanged. See [Compatibility](../../reference/compatibility/).

`StickyPartitioner` tracks each broker node's Produce latency and, by
default, rotates unkeyed records toward whichever candidate's leader has
responded fastest rather than choosing uniformly at random — the same idea
as [`partitioner.adaptive.partitioning.enable`](https://kafka.apache.org/43/configuration/producer-configs/#partitioner.adaptive.partitioning.enable).
It falls back to a plain uniform choice whenever none of the candidates have
been measured yet (e.g. right after a rebalance). Disable it with
`createPartitioner: () => Partitioners.StickyPartitioner({ adaptive: false })`.
A custom partitioner can read the same signal via `PartitionerArgs.nodeLatency`.

## Idempotence and abort

`idempotent` defaults to `false` — an explicit opt-in. `send()` and
`sendBatch()` accept `signal` to abort the wait.

```ts
await producer.send({ topic: 'events', messages: [{ value: 'hello' }], signal });
```

Transactions: [Producer API](../../reference/producer/#transaction).
